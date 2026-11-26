//! Reading a capture output tree back: the file listing, and the check that
//! every manifest describes the bytes beside it.

use std::{
    fs,
    path::{Path, PathBuf},
};

use md_pipeline::{IngestError, IngestSource};
use md_replay::CaptureReplay;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Every file under `root` as (relative path, bytes), sorted by path so two
/// trees compare independent of read-dir order.
pub fn output_tree(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(dir: &Path, base: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        let mut paths: Vec<_> = fs::read_dir(dir)
            .expect("read_dir")
            .map(|e| e.expect("dir entry").path())
            .collect();
        paths.sort();
        for p in paths {
            if p.is_dir() {
                walk(&p, base, out);
            } else {
                let rel = p
                    .strip_prefix(base)
                    .expect("under base")
                    .to_string_lossy()
                    .into_owned();
                out.push((rel, fs::read(&p).expect("read file")));
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn manifest_dirs(root: &Path, found: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).expect("read_dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            manifest_dirs(&path, found);
        } else if path.file_name().is_some_and(|n| n == "manifest.json") {
            found.push(root.to_path_buf());
        }
    }
}

/// Number of rows in the capture container at `path`. The zstd body must decode
/// to its end, so a file cut short fails here.
pub fn captured_rows(path: &Path) -> u64 {
    let file = fs::File::open(path).expect("open capture");
    let mut source = CaptureReplay::from_reader(file)
        .unwrap_or_else(|err| panic!("{} does not decode: {err}", path.display()));
    let mut rows = 0;
    let mut out = Vec::new();
    loop {
        out.clear();
        match source.recv_batch(&mut out, 256) {
            Ok(n) => rows += n as u64,
            Err(IngestError::Closed) => return rows,
            Err(err) => panic!("{}: {err}", path.display()),
        }
    }
}

/// Checks every `manifest.json` under `root`: each listed file exists, its
/// sha256 matches its bytes, its record count matches the rows it holds, and
/// no capture leaf or staging file sits outside the manifests. Returns the
/// number of files the manifests list.
pub fn assert_manifests_verify(root: &Path) -> usize {
    let mut dirs = Vec::new();
    manifest_dirs(root, &mut dirs);
    assert!(
        !dirs.is_empty(),
        "no manifest.json under {}",
        root.display()
    );

    let mut listed = Vec::new();
    for dir in &dirs {
        let manifest: Value =
            serde_json::from_slice(&fs::read(dir.join("manifest.json")).expect("read manifest"))
                .expect("manifest parses");
        for file in manifest["files"].as_array().expect("files array") {
            let leaf = dir.join(file["path"].as_str().expect("path"));
            let bytes = fs::read(&leaf).unwrap_or_else(|_| panic!("{} missing", leaf.display()));
            let digest: String = Sha256::digest(&bytes)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            assert_eq!(file["sha256"], digest, "{} sha256", leaf.display());
            assert_eq!(
                file["records"].as_u64(),
                Some(captured_rows(&leaf)),
                "{} record count",
                leaf.display()
            );
            listed.push(leaf);
        }
    }

    for (rel, _) in output_tree(root) {
        let path = root.join(&rel);
        let is_leaf = rel.ends_with(".mdc1.zst");
        assert!(
            !rel.ends_with(".tmp") && (!is_leaf || listed.contains(&path)),
            "{rel} is not covered by a manifest"
        );
    }
    listed.len()
}
