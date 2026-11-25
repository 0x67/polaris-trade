use super::*;

// --- C header emitter (workstream B2) --------------------------------------------

fn c_uint(repr: EnumRepr) -> &'static str {
    match repr {
        EnumRepr::U8 => "uint8_t",
        EnumRepr::U16 => "uint16_t",
    }
}

fn c_scalar(c: &FieldClass) -> &'static str {
    match c {
        FieldClass::U8 => "uint8_t",
        FieldClass::U16 => "uint16_t",
        FieldClass::U32 => "uint32_t",
        FieldClass::U64 => "uint64_t",
        FieldClass::I8 => "int8_t",
        FieldClass::I16 => "int16_t",
        FieldClass::I32 => "int32_t",
        FieldClass::I64 | FieldClass::Price => "int64_t",
        FieldClass::Flags => "uint16_t",
        FieldClass::Bytes { .. }
        | FieldClass::Enum { .. }
        | FieldClass::Struct { .. }
        | FieldClass::Array { .. } => "uint8_t",
    }
}

fn emit_c_field(name: &str, class: &FieldClass, enum_reprs: &Layouts, out: &mut String) {
    match class {
        FieldClass::Bytes { width } => {
            out.push_str(&format!("    uint8_t {name}[{width}];\n"));
        }
        FieldClass::Array { elem, count } => match &**elem {
            FieldClass::Struct { ty } => {
                out.push_str(&format!("    {ty} {name}[{count}];\n"));
            }
            scalar => {
                out.push_str(&format!("    {} {name}[{count}];\n", c_scalar(scalar)));
            }
        },
        FieldClass::Enum { ty } => {
            let repr = enum_reprs.enum_reprs[ty];
            out.push_str(&format!("    {} {name}; /* {ty} */\n", c_uint(repr)));
        }
        FieldClass::Struct { ty } => {
            out.push_str(&format!("    {ty} {name};\n"));
        }
        scalar => {
            out.push_str(&format!("    {} {name};\n", c_scalar(scalar)));
        }
    }
}

/// emit one C `typedef struct` + `_Static_assert`. `hdr` prepends the shared header.
fn emit_c_struct(name: &str, hdr: bool, layout: &Layout, layouts: &Layouts, out: &mut String) {
    out.push_str(&format!("typedef struct {name} {{\n"));
    if hdr {
        out.push_str("    RecordHeader hdr;\n");
    }
    for slot in &layout.slots {
        match slot {
            Slot::Field { name, class, .. } => emit_c_field(name, class, layouts, out),
            Slot::Pad { name, width, .. } => {
                out.push_str(&format!("    uint8_t {name}[{width}];\n"));
            }
        }
    }
    out.push_str(&format!("}} {name};\n"));
    out.push_str(&format!(
        "MD_STATIC_ASSERT(sizeof({name}) == {}, \"{name} layout drift\");\n\n",
        layout.size
    ));
}

pub(super) fn emit_c_header(s: &RecordSchema, layouts: &Layouts, prov: &str) -> String {
    let mut out = prov.to_string();
    out.push_str("#ifndef MD_EVENTS_H\n#define MD_EVENTS_H\n\n");
    out.push_str("#include <stdint.h>\n\n");
    // self-contained static-assert: `_Static_assert` is a C11 keyword (no header), and
    // C++ has the `static_assert` keyword. Avoiding <assert.h> keeps the header free of
    // system includes, which header-parsing binding tools (ctypesgen, c-for-go) prefer.
    out.push_str(
        "#ifdef __cplusplus\n\
         #define MD_STATIC_ASSERT(cond, msg) static_assert(cond, msg)\n\
         #else\n\
         #define MD_STATIC_ASSERT(cond, msg) _Static_assert(cond, msg)\n\
         #endif\n\n",
    );
    out.push_str("#ifdef __cplusplus\nextern \"C\" {\n#endif\n\n");

    out.push_str("/* enums: width-exact storage typedef plus integer value constants. */\n");
    for e in &s.enums {
        out.push_str(&format!("typedef {} {};\n", c_uint(e.repr), e.name));
        out.push_str("enum {\n");
        for v in &e.variants {
            let value = match e.repr {
                EnumRepr::U8 => v.value.to_string(),
                EnumRepr::U16 => format!("{:#06X}", v.value),
            };
            out.push_str(&format!("    {}_{} = {value},\n", e.name, v.name));
        }
        out.push_str("};\n\n");
    }

    if !s.flags.is_empty() {
        out.push_str("/* RecordFlags bits. */\n");
        for fl in &s.flags {
            out.push_str(&format!(
                "#define MD_FLAG_{} {:#06x}\n",
                fl.name,
                1u32 << fl.bit
            ));
        }
        out.push('\n');
    }

    emit_c_struct(&s.header.name, false, &layouts.header, layouts, &mut out);
    for st in &s.structs {
        emit_c_struct(
            &st.name,
            false,
            &layouts.structs[&st.name],
            layouts,
            &mut out,
        );
    }
    for r in &s.records {
        emit_c_struct(&r.name, true, &layouts.records[&r.name], layouts, &mut out);
    }

    out.push_str("#ifdef __cplusplus\n}\n#endif\n\n");
    out.push_str("#endif\n");
    out
}
