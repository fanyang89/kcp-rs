/* Test-only subprocess bridge: no C code is linked into the Rust crate. */
#include "ikcp.h"
#include <stdint.h>
#include <stdio.h>

static void frame(int tag, const char *data, uint32_t len) {
    unsigned char h[5] = {(unsigned char)tag, len, len >> 8, len >> 16, len >> 24};
    fwrite(h, 1, 5, stdout);
    if (len) fwrite(data, 1, len, stdout);
}
static int output(const char *data, int len, ikcpcb *kcp, void *user) {
    (void)kcp; (void)user;
    frame('p', data, len); return 0;
}
static uint32_t read32(const unsigned char *p) {
    return (uint32_t)p[0] | (uint32_t)p[1]<<8 | (uint32_t)p[2]<<16 | (uint32_t)p[3]<<24;
}
int main(void) {
    ikcpcb *kcp = ikcp_create(7, NULL);
    unsigned char h[9];
    char buf[262144];
    int hold_reads = 0;
    if (!kcp) return 1;
    ikcp_setoutput(kcp, output);
    ikcp_nodelay(kcp, 1, 10, 2, 0);
    while (fread(h, 1, 9, stdin) == 9) {
        uint32_t now = read32(h+1), len = read32(h+5);
        if (len > sizeof(buf) || fread(buf, 1, len, stdin) != len) return 2;
        int rc = 0;
        if (h[0] == 'c') {
            if (len != 32) return 5;
            uint32_t values[8];
            for (int i = 0; i < 8; i++) values[i] = read32((unsigned char *)buf + 4*i);
            if (values[0] < 50 || values[0] > 65535 || values[1] > 1 ||
                values[2] < 10 || values[2] > 5000 || values[3] > 65535 ||
                values[4] > 1 || values[5] > 1 || values[6] == 0 || values[6] > 65535 ||
                values[7] < 128 || values[7] > 65535) return 6;
            rc = ikcp_setmtu(kcp, values[0]);
            ikcp_nodelay(kcp, values[1], values[2], values[3], values[4]);
            kcp->stream = values[5];
            ikcp_wndsize(kcp, values[6], values[7]);
        }
        else if (h[0] == 'h') { if (len != 0) return 7; hold_reads = now != 0; }
        else if (h[0] == 'q') {
            if (len != 0) return 8;
            uint32_t values[2] = {(uint32_t)ikcp_waitsnd(kcp), kcp->rmt_wnd};
            unsigned char stats[8];
            for (int i = 0; i < 2; i++) for (int j = 0; j < 4; j++) stats[4*i+j] = values[i] >> (8*j);
            frame('w', (char *)stats, sizeof(stats));
        }
        else if (h[0] == 's') {
            rc = ikcp_send(kcp, buf, len);
            /* Older reference revisions return 0, newer ones return bytes accepted. */
            if (rc > 0 && (uint32_t)rc != len) return 9;
        }
        else if (h[0] == 'i') { kcp->current = now; rc = ikcp_input(kcp, buf, len); }
        else if (h[0] == 'u') ikcp_update(kcp, now);
        else return 3;
        if (rc < 0) return 4;
        while (!hold_reads && (rc = ikcp_recv(kcp, buf, sizeof(buf))) >= 0) frame('m', buf, rc);
        frame('e', NULL, 0); fflush(stdout);
    }
    ikcp_release(kcp);
    return 0;
}
