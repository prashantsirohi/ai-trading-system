"""Template YAML files for `ai-trading-optimize init`.

Templates use `string.Template`-style `$name` placeholders so we don't fight
YAML's curly braces. Placeholders supported:

  $name           — recipe + rule pack name (operator-supplied)
  $strategy_id    — strategy_id field; defaults to $name
  $today          — today's ISO date (filled in by cli.py for the recipe range)
"""
