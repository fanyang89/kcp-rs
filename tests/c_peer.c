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
    if (!kcp) return 1;
    ikcp_setoutput(kcp, output);
    ikcp_nodelay(kcp, 1, 10, 2, 0);
    while (fread(h, 1, 9, stdin) == 9) {
        uint32_t now = read32(h+1), len = read32(h+5);
        if (len > sizeof(buf) || fread(buf, 1, len, stdin) != len) return 2;
        int rc = 0;
        if (h[0] == 's') rc = ikcp_send(kcp, buf, len);
        else if (h[0] == 'i') { kcp->current = now; rc = ikcp_input(kcp, buf, len); }
        else if (h[0] == 'u') ikcp_update(kcp, now);
        else return 3;
        if (rc < 0) return 4;
        while ((rc = ikcp_recv(kcp, buf, sizeof(buf))) >= 0) frame('m', buf, rc);
        frame('e', NULL, 0); fflush(stdout);
    }
    ikcp_release(kcp);
    return 0;
}
