from mv_lineage.cli import parse_args


def test_parse_args_requires_database_and_output():
    args = parse_args(["--database", "peak", "--output", "lineage.html"])
    assert args.database == "peak"
    assert args.output == "lineage.html"
