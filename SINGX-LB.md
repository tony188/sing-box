# SingX LB fork of sing-box

This is a fork of upstream [SagerNet/sing-box](https://github.com/SagerNet/sing-box)
at tag `v1.14.4` (`c005f35300ce76427dc285481ec5695f5b179cd2`), maintained for the
SingX Android client. It adds a `loadbalance` outbound type (least-active-
connections first, latency window 20ms/20%, hedged dialling, health state
machine) plus the probe-rewrite groundwork it depends on.

## Origin of the patches

All 14 commits on branch `singx-lb-1.14.4` are ported from the Open-Box
kernel source package `sing-box-1.14.4-openbox-44-source.tar.gz`
(`sing-box-1.14.4-openbox-44`, GPL-3.0; sing-box itself is GPL-3.0). Each
commit message names the Open-Box patch file it came from. The patches are
applied unchanged, except:

- `low-memory`: only the `urltest.go` part + new file
  `protocol/group/openbox_workers.go` are ported (the `route/rule/rule_set_local.go`
  part, .srs-watch skipping for iOS, is not ported).
- The `bandwidth` selection code is included, but SingX configuration never
  sets `bandwidth: true` (the download probe lives in the closed-source
  Open-Box backend, not in this kernel).

No other Open-Box patches are included. The fork contains no closed-source
Open-Box code.

## Verified build

`libbox-lb.aar` built from this branch with the TRIM gomobile bind:

- Go 1.25.5, gomobile v0.1.12 (SagerNet fork), JDK 17, NDK 27.2.12479018
- `gomobile bind -target android/arm64,android/arm -androidapi 24
  -javapkg=io.nekohasekai -libname=box
  -tags with_gvisor,with_quic,with_wireguard,with_utls,with_clash_api,badlinkname,tfogo_checklinkname0
  -ldflags "-X github.com/sagernet/sing-box/constant.Version=1.14.4
  -X runtime.godebugDefault=multipathtcp=0,tlssha1=1 -checklinkname=0 -s -w -buildid="
  ./experimental/libbox`
  with `GOFLAGS="-trimpath -buildvcs=false"` — the bind MUST use BOTH
  flags: `-trimpath` (without it the absolute build directory is embedded
  in the `.so`, 572 occurrences, so the same tree built from a different
  directory produces a different AAR sha) and `-buildvcs=false` (without
  it Go VCS stamping embeds the build-time git state, so an AAR built from
  an uncommitted tree can never be reproduced from a committed branch).
  The first G1b build (no `-trimpath`) produced sha `8a2de809…107b`
  (path-specific, superseded); the `-trimpath`-only build produced
  `a39e764a…522ba` (commit-specific via VCS stamping, superseded).
- `go/*` (gomobile Seq runtime) stripped from `classes.jar` — it is provided
  once by the separate `go-seq.jar`, as in the SingX tree.

Result (verified, reproduced byte-identically from two different build
directories AND from committed vs uncommitted trees):

- `libbox-lb.aar`: 39,915,113 bytes
- sha256: `8f75be61290c396b4ffee7a3debddacdf35d721d121c6cb66decb4fc8c1b1da6`

Checks run on this branch before the AAR build: `go vet`, `go test`
(`TestOpenBoxLoadBalance`, `TestOpenBoxURLTestWorkers`,
`TestOpenBox(Probe|Bandwidth|Confirm|Expected)`), `go build ./...`, and a
`checkConfig` dry run (a config with a `loadbalance` group passes; 65
members are rejected; a selector used as a member passes `checkConfig` and
is rejected at `Start()` with `load balance requires real proxy nodes`).

## Layout

- Branch `singx-lb-1.14.4`: 14 patch commits + this note, on top of `v1.14.4`.
- Nothing else in this fork differs from upstream `v1.14.4`.
