#!/usr/bin/env bash

set -euo pipefail

readonly REPORT_DIR="reports/codequality"
readonly RAW_DIR="${REPORT_DIR}/raw"
readonly COVER_FILE="${REPORT_DIR}/coverage.txt"
readonly CYCLO_FILE="${REPORT_DIR}/cyclomatic-red.txt"
readonly COGN_FILE="${REPORT_DIR}/cognitive-red.txt"
readonly DUPL_FILE="${REPORT_DIR}/duplication.txt"

readonly GREEN="\033[0;32m"
readonly YELLOW="\033[0;33m"
readonly RED="\033[0;31m"
readonly RESET="\033[0m"

run_quality_inputs() {
  mkdir -p "${RAW_DIR}"

  go test ./cmd/... ./internal/... \
    -coverprofile="${RAW_DIR}/unit-coverage.out" \
    -coverpkg=./cmd/...,./internal/... \
    >/dev/null

  go tool cover -func="${RAW_DIR}/unit-coverage.out" >"${COVER_FILE}"

  # gocyclo/gocognit return non-zero when findings exceed threshold.
  go tool gocyclo -over 15 ./internal ./cmd >"${CYCLO_FILE}" 2>/dev/null || true
  go tool gocognit -over 20 ./internal ./cmd >"${COGN_FILE}" 2>/dev/null || true
  go tool dupl -t 60 ./internal ./cmd >"${DUPL_FILE}"
}

coverage_value() {
  awk '/^total:/ {gsub("%","",$3); print $3}' "${COVER_FILE}"
}

line_count_or_zero() {
  local file_path="$1"
  awk 'NF>0 {n++} END {print n+0}' "${file_path}"
}

duplication_group_count() {
  awk '/Found total/ {print $3}' "${DUPL_FILE}"
}

metric_color() {
  local status="$1"
  case "${status}" in
    GREEN) printf "%b" "${GREEN}" ;;
    YELLOW) printf "%b" "${YELLOW}" ;;
    *) printf "%b" "${RED}" ;;
  esac
}

coverage_status() {
  local value="$1"
  if awk "BEGIN {exit !(${value} >= 70)}"; then
    printf "GREEN"
  elif awk "BEGIN {exit !(${value} >= 50)}"; then
    printf "YELLOW"
  else
    printf "RED"
  fi
}

count_status() {
  local value="$1"
  local green_max="$2"
  local yellow_max="$3"

  if [ "${value}" -le "${green_max}" ]; then
    printf "GREEN"
  elif [ "${value}" -le "${yellow_max}" ]; then
    printf "YELLOW"
  else
    printf "RED"
  fi
}

print_metric_line() {
  local label="$1"
  local value="$2"
  local suffix="$3"
  local status="$4"
  local color

  color="$(metric_color "${status}")"
  printf "  %-24s %b%s%s%b  (%s)\n" "${label}" "${color}" "${value}" "${suffix}" "${RESET}" "${status}"
}

print_report() {
  local coverage="$1"
  local cyclo="$2"
  local cogn="$3"
  local dupl="$4"

  local cover_status
  local cyclo_status
  local cogn_status
  local dupl_status

  cover_status="$(coverage_status "${coverage}")"
  cyclo_status="$(count_status "${cyclo}" 0 10)"
  cogn_status="$(count_status "${cogn}" 0 10)"
  dupl_status="$(count_status "${dupl}" 1 5)"

  printf "\nCodequality Report\n"
  print_metric_line "Coverage total:" "${coverage}" "%" "${cover_status}"
  print_metric_line "Cyclomatic > 15 count:" "${cyclo}" "" "${cyclo_status}"
  print_metric_line "Cognitive > 20 count:" "${cogn}" "" "${cogn_status}"
  print_metric_line "Duplication clone groups:" "${dupl}" "" "${dupl_status}"
  printf "\n"
}

main() {
  run_quality_inputs

  local coverage
  local cyclo
  local cogn
  local dupl

  coverage="$(coverage_value)"
  cyclo="$(line_count_or_zero "${CYCLO_FILE}")"
  cogn="$(line_count_or_zero "${COGN_FILE}")"
  dupl="$(duplication_group_count)"

  print_report "${coverage}" "${cyclo}" "${cogn}" "${dupl}"
}

main "$@"
