from mv_lineage.cli import parse_args


def test_parse_args_requires_database_and_output():
    args = parse_args(["--database", "peak", "--output", "lineage.html"])
    assert args.database == "peak"
    assert args.output == "lineage.html"


def test_parse_args_with_log_options():
    args = parse_args(
        [
            "--database",
            "peak",
            "--output",
            "lineage.html",
            "--log-hours",
            "24",
            "--log-limit-per-node",
            "20",
        ]
    )
    assert args.log_hours == 24
    assert args.log_limit_per_node == 20
    assert args.disable_node_logs is False
