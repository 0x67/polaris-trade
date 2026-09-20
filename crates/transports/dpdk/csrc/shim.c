/*
 * Linkable wrappers over DPDK static-inline receive helpers, which export no symbol.
 * rte_mbuf and rte_mempool cross as opaque pointers; bindings in src/ffi.rs.
 */

#include <rte_ethdev.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>

/* one call per burst: take up to nb mbufs, then each one's data pointer, length, segment count */
uint16_t polaris_dpdk_rx_burst(uint16_t port, uint16_t queue, struct rte_mbuf **mbufs,
                               const uint8_t **data, uint16_t *len, uint16_t *nb_segs,
                               uint16_t nb)
{
    uint16_t n = rte_eth_rx_burst(port, queue, mbufs, nb);
    for (uint16_t i = 0; i < n; i++) {
        data[i] = rte_pktmbuf_mtod(mbufs[i], const uint8_t *);
        len[i] = rte_pktmbuf_data_len(mbufs[i]);
        nb_segs[i] = mbufs[i]->nb_segs;
    }
    return n;
}

void polaris_dpdk_free(struct rte_mbuf *m)
{
    rte_pktmbuf_free(m);
}

/* port-wide counters; outputs untouched when read fails, so caller keeps last good values */
void polaris_dpdk_rx_drops(uint16_t port, uint64_t *imissed, uint64_t *rx_nombuf)
{
    struct rte_eth_stats stats;
    if (rte_eth_stats_get(port, &stats) == 0) {
        *imissed = stats.imissed;
        *rx_nombuf = stats.rx_nombuf;
    }
}

/* in_use walks every lcore cache: debug only */
void polaris_dpdk_pool_stats(const struct rte_mempool *mp, unsigned int *capacity,
                             unsigned int *in_use)
{
    *capacity = mp->size;
    *in_use = rte_mempool_in_use_count(mp);
}
