# YAML Configuration Migration Design

## Goal

Replace the Windows follower's JSON runtime configuration with a commented YAML configuration that is easier to read and edit.

## Scope

- Add `PyYAML` as the configuration parser dependency.
- Replace `config.example.json` with `config.example.yaml`, using native YAML comments beside the relevant settings.
- Change the default CLI configuration path to `config.yaml` and update only documentation that tells users how to copy, edit, or pass the file.
- Load YAML mappings while preserving all existing field names, defaults, `${ENV_VAR}` password resolution, and type conversions.
- Update the runtime configuration test to exercise YAML input and verify the checked-in YAML template parses.

## Compatibility and safety

JSON configuration files are intentionally no longer supported. Operators must copy the new template to `config.yaml` and migrate any existing local values. The migration does not change Redis, QMT, FIFO execution, pricing, risk controls, SQLite state, or signal contracts.

The loader will require `PyYAML`; it will not silently fall back to JSON or a partial parser. Existing local `config.json` remains untouched and ignored by Git.

## Files

- `qmt_follower/config.py`: replace JSON parsing with `yaml.safe_load`.
- `qmt_follower/app.py`: update the default path and help text.
- `config.example.yaml`: YAML template with native Chinese comments.
- `tests/test_runtime.py`: regression coverage for YAML loading and the example template.
- `README.md`, `docs/windows-qmt-adapter-handoff.md`, `docs/joinquant-community-promo.md`, `AGENTS.md`, and the QMT adapter's configuration error: replace JSON file-name and format references that are part of the operator workflow.

## Acceptance criteria

1. `python main.py` loads `config.yaml` by default.
2. A YAML configuration preserves the current `RuntimeConfig` values, including environment-variable password resolution.
3. `config.example.yaml` parses with `yaml.safe_load` and has no `_comment` keys.
4. Operator documentation uses the YAML template and its native comments.
5. JSON configuration is rejected rather than treated as a supported compatibility path.
