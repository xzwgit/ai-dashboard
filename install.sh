#!/usr/bin/env bash
# vllm-dashboard 安装脚本 —— 同一仓库多版本，手动选版本安装
#
# 用法:
#   ./install.sh v1              安装 v1（纯监控大屏）
#   ./install.sh v2              安装 v2（监控 + API 分发网关）
#   ./install.sh v3              安装 v3（监控 + 网关负载均衡 + 负载均衡总表）
#   ./install.sh latest          安装最新开发版（master）
#   ./install.sh v2.0.0          安装指定 tag（打 tag 发布后可用）
#   ./install.sh list            列出仓库已发布的版本 tag
#   ./install.sh v2 /opt/dash    第二参数 = 安装目录（默认当前目录），落成 <目录>/vllm_dashboard.py
#
# 一键式（不先落脚本）:
#   curl -fsSL https://raw.githubusercontent.com/xzwgit/vllm-dashboard/master/install.sh | bash -s v2
#   （raw 被墙时先落脚本可改走：https://github.com/xzwgit/vllm-dashboard/raw/master/install.sh）
#
# 下载源自动回退（无需配置）: raw tag → raw master 冻结副本 → GitHub API(tag) → GitHub API(master)。
# raw.githubusercontent.com 不可达的网络上会自动落到 api.github.com（国内常见形态）。
#
# 环境变量:
#   VD_PROXY=<镜像前缀>   所有 GitHub 请求加代理前缀，如:
#                         VD_PROXY=https://mirror.ghproxy.com ./install.sh v2
#   VD_BASE=<base>        raw 换源（默认 https://raw.githubusercontent.com/xzwgit/vllm-dashboard；测试用）
#   VD_API_BASE=<base>    GitHub API 换源（默认 https://api.github.com/repos/xzwgit/vllm-dashboard；测试用）
set -euo pipefail

REPO_BASE="${VD_BASE:-https://raw.githubusercontent.com/xzwgit/vllm-dashboard}"
API_BASE="${VD_API_BASE:-https://api.github.com/repos/xzwgit/vllm-dashboard}"
DEST_NAME="vllm_dashboard.py"

say() { printf '%s\n' "$*"; }
die() { printf '\n[错误] %s\n' "$*" >&2; exit 1; }

usage() { # 打印文件头注释块（到 set 行为止）
  awk 'NR==1{next} /^set /{exit} {sub(/^# ?/,""); print}' "$0"
}

fetch() { # fetch <url> <输出文件>
  local url="$1" out="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL --connect-timeout 10 "$url" -o "$out"
  elif command -v wget >/dev/null 2>&1; then
    wget -q --timeout=20 "$url" -O "$out"
  else
    die "需要 curl 或 wget 任一可用"
  fi
}

full_url() { printf '%s%s' "${VD_PROXY:-}" "$1"; }

do_list() {
  local tmp; tmp=$(mktemp)
  if ! fetch "$(full_url "$API_BASE/tags")" "$tmp"; then
    die "拉取版本列表失败（GitHub API 不可达？可设 VD_PROXY 换源，或直接 ./install.sh v1|v2）"
  fi
  local tags
  tags=$(grep -oE '"name"[: ]*"[^"]+"' "$tmp" | sed 's/.*"\(.*\)"$/\1/' || true)
  rm -f "$tmp"
  if [ -z "$tags" ]; then
    say "仓库还没有打任何 tag。"
    say "仍可安装: ./install.sh v1 | ./install.sh v2 | ./install.sh latest"
  else
    say "已发布版本:"
    say "$tags" | sed 's/^/  /'
    say "安装: ./install.sh <上面任一 tag>"
  fi
}

# 解析版本参数 → (REF, FILE) 主来源 / (FB_REF, FB_FILE) 回退来源
resolve() {
  local v="$1"
  FILE="$DEST_NAME"
  case "$v" in
    v1)
      REF="v1.0.0"; FB_REF="master"; FB_FILE="vllm_dashboard_v1.py"
      DESC="v1（纯监控大屏）"
      ;;
    v2)
      REF="v2.0.0"; FB_REF="master"; FB_FILE="vllm_dashboard.py"
      DESC="v2（监控 + API 分发网关）"
      ;;
    v3)
      REF="v3.0.0"; FB_REF="master"; FB_FILE="vllm_dashboard.py"
      DESC="v3（监控 + 网关负载均衡）"
      ;;
    latest)
      REF="master"; FB_REF=""; FB_FILE=""
      DESC="latest（master 开发版，可能不稳定）"
      ;;
    v[0-9]*)
      REF="$v"; FB_REF=""; FB_FILE=""
      DESC="$v（指定 tag）"
      ;;
    *)
      usage >&2
      die "未知版本: $v（可选 v1 / v2 / latest / vX.Y.Z / list）"
      ;;
  esac
}

try_raw() { # try_raw <ref> <file>
  fetch "$(full_url "$REPO_BASE/$1/$2")" "$TMP"
}

try_api() { # try_api <ref> <file> —— contents API 返回 base64 JSON，用 python 解码（python3 或 python）
  [ -n "$PY_BIN" ] || return 1
  local J; J=$(mktemp)
  if ! fetch "$(full_url "$API_BASE/contents/$2?ref=$1")" "$J"; then rm -f "$J"; return 1; fi
  "$PY_BIN" -c "import json,sys; d=json.load(open(sys.argv[1],'rb')); sys.exit(0 if 'content' in d and d.get('encoding')=='base64' else 1)" "$J" \
    || { rm -f "$J"; return 1; }
  "$PY_BIN" -c "import json,sys,base64; d=json.load(open(sys.argv[1],'rb')); open(sys.argv[2],'wb').write(base64.b64decode(d['content']))" "$J" "$TMP"
  local rc=$?; rm -f "$J"; return $rc
}

# 探测可用的 python（python3 优先；Windows 商店的 python3 假别名跑不起来，要实测才认）
PY_BIN=""
for _c in python3 python; do
  if command -v "$_c" >/dev/null 2>&1 && "$_c" -c "import sys" >/dev/null 2>&1; then
    PY_BIN="$_c"; break
  fi
done

[ $# -ge 1 ] || { usage; die "请指定版本：v1 / v2 / latest / vX.Y.Z / list"; }

if [ "$1" = "list" ]; then do_list; exit 0; fi

resolve "$1"
DEST_DIR="${2:-.}"
mkdir -p "$DEST_DIR"
DEST="$DEST_DIR/$DEST_NAME"
TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT

SRC=""
if try_raw "$REF" "$FILE"; then
  SRC="$REPO_BASE/$REF/$FILE"
elif [ -n "$FB_REF" ] && try_raw "$FB_REF" "$FB_FILE"; then
  SRC="$REPO_BASE/$FB_REF/$FB_FILE（回退：tag 未打或不可达）"
elif try_api "$REF" "$FILE"; then
  SRC="GitHub API ref=$REF（raw 被墙兜底）"
elif [ -n "$FB_REF" ] && try_api "$FB_REF" "$FB_FILE"; then
  SRC="GitHub API ref=$FB_REF/$FB_FILE（raw 被墙兜底）"
else
  die "下载失败（raw 与 API 均不可达）：$REPO_BASE/$REF/$FILE
可尝试: 1) VD_PROXY=<镜像前缀> $0 $1
        2) ./install.sh list 查看已发布版本（tag 是否存在/拼错）
        3) 网络直连 GitHub 的机器上先下载再 scp 过去"
fi

# 校验下载物确实是我们这个单文件工具（含 VERSION 常量），防止代理/网关返回错误页
VER=$(grep -m1 '^VERSION' "$TMP" | sed 's/.*"\(.*\)".*/\1/')
[ -n "$VER" ] || die "下载内容校验失败（未找到 VERSION 常量，可能拿到了错误页）：$SRC"

# 已有旧文件先备份，不静默覆盖
if [ -f "$DEST" ]; then
  OLD_VER=$(grep -m1 '^VERSION' "$DEST" | sed 's/.*"\(.*\)".*/\1/' || true)
  BAK="$DEST.bak.${OLD_VER:-unknown}.$(date +%Y%m%d%H%M%S)"
  cp "$DEST" "$BAK"
  say "已备份旧文件: $BAK"
fi

mv "$TMP" "$DEST"; trap - EXIT

say "✓ 安装完成  版本 $VER（$DESC）"
say "  来源: $SRC"
say "  文件: $DEST"

if [ -z "$PY_BIN" ]; then
  say "  [提醒] 本机没有 python3/python，运行前需要装（工具零第三方依赖，Python 3.6+ 即可）"
fi

say ""
say "下一步:"
case "$VER" in
  1.*)
    say "  python3 $DEST --target 名字=http://<vLLM地址>:8123     # 纯监控大屏（默认端口 8124）"
    say "  python3 $DEST --demo                                    # 无服务时预览界面"
    ;;
  *)
    say "  python3 $DEST --target 名字=http://<vLLM地址>:8123     # 纯监控大屏（默认端口 8124）"
    say "  python3 $DEST --gw --target 名字=http://<vLLM地址>:8123 # 监控 + API 分发网关（/gw/v1，端口 8125）"
    ;;
esac
