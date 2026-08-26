import io

from nyc_data_analysis_project import main


class TestYearMonths:
    def test_full_range_matches_the_real_published_count(self):
        # Sanity-checked against the actual NYC TLC listing earlier in this
        # project's development (grep'd off the real page): there are exactly
        # 209 monthly files from 2009-01 through 2026-05. If this count ever
        # drifts, it's a sign START_YEAR_MONTH/END_YEAR_MONTH is out of sync
        # with what TLC actually publishes.
        months = list(main._year_months(main.START_YEAR_MONTH, main.END_YEAR_MONTH))
        assert len(months) == 209
        assert months[0] == (2009, 1)
        assert months[-1] == (2026, 5)

    def test_year_rolls_over_at_december(self):
        # The Dec -> Jan rollover is the one place an off-by-one is easy to
        # introduce (e.g. incrementing year without resetting month to 1).
        months = list(main._year_months((2020, 11), (2021, 2)))
        assert months == [(2020, 11), (2020, 12), (2021, 1), (2021, 2)]


class TestEraSubfolder:
    def test_2009_is_legacy_2009(self):
        assert main._era_subfolder(2009) == "legacy_2009"

    def test_2010_is_legacy_2010(self):
        assert main._era_subfolder(2010) == "legacy_2010"

    def test_boundary_years_around_legacy_are_modern(self):
        # 2011 is the first "modern" year (bronze's MODERN_COLUMNS mapping
        # starts there) and 2026 is the latest currently published — both must
        # route to "modern", not accidentally fall into a legacy bucket.
        assert main._era_subfolder(2011) == "modern"
        assert main._era_subfolder(2026) == "modern"


class TestLandYellowTripdata:
    def _patch_single_month(self, monkeypatch, year=2015, month=1):
        monkeypatch.setattr(main, "START_YEAR_MONTH", (year, month))
        monkeypatch.setattr(main, "END_YEAR_MONTH", (year, month))

    def _patch_urlopen(self, monkeypatch, content=b"downloaded-bytes"):
        calls = []

        def fake_urlopen(url):
            calls.append(url)
            return io.BytesIO(content)

        monkeypatch.setattr(main.urllib.request, "urlopen", fake_urlopen)
        return calls

    def test_skips_download_when_file_already_landed(self, tmp_path, monkeypatch):
        # This is the whole point of the idempotency check — re-running the
        # landing job must not re-download the ~100+GB of already-landed
        # history. A file already sitting in the correct era subfolder must
        # never trigger a network call.
        self._patch_single_month(monkeypatch)
        calls = self._patch_urlopen(monkeypatch)

        era_dir = tmp_path / "modern"
        era_dir.mkdir()
        existing = era_dir / "yellow_tripdata_2015-01.parquet"
        existing.write_bytes(b"already-here")

        main._land_yellow_tripdata(str(tmp_path))

        assert calls == []
        assert existing.read_bytes() == b"already-here"

    def test_moves_file_from_old_flat_layout_instead_of_redownloading(self, tmp_path, monkeypatch):
        # Regression test: files landed before the era-subfolder layout existed
        # sit flat directly under volume_path. The fix for that (this session)
        # was to move them into place rather than re-fetch ~100+GB from
        # CloudFront — a network call here means that fix regressed.
        self._patch_single_month(monkeypatch)
        calls = self._patch_urlopen(monkeypatch)

        tmp_path.mkdir(exist_ok=True)
        flat_file = tmp_path / "yellow_tripdata_2015-01.parquet"
        flat_file.write_bytes(b"landed-before-subfolders-existed")

        main._land_yellow_tripdata(str(tmp_path))

        assert calls == []
        assert not flat_file.exists()
        moved = tmp_path / "modern" / "yellow_tripdata_2015-01.parquet"
        assert moved.read_bytes() == b"landed-before-subfolders-existed"

    def test_downloads_when_file_is_missing_everywhere(self, tmp_path, monkeypatch):
        self._patch_single_month(monkeypatch)
        calls = self._patch_urlopen(monkeypatch, content=b"fresh-download")

        main._land_yellow_tripdata(str(tmp_path))

        assert len(calls) == 1
        assert calls[0] == f"{main.BASE_URL}/yellow_tripdata_2015-01.parquet"
        landed = tmp_path / "modern" / "yellow_tripdata_2015-01.parquet"
        assert landed.read_bytes() == b"fresh-download"

    def test_download_writes_via_tmp_file_not_directly_to_destination(self, tmp_path, monkeypatch):
        # If the process dies mid-download, a half-written file must not be
        # mistaken for a complete one on the next run (which would silently
        # skip it forever, per the "already exists" check). The .tmp ->
        # os.rename sequence is what guarantees the final filename only ever
        # appears once the file is fully written.
        self._patch_single_month(monkeypatch)
        self._patch_urlopen(monkeypatch, content=b"fresh-download")

        main._land_yellow_tripdata(str(tmp_path))

        era_dir = tmp_path / "modern"
        assert [p.name for p in era_dir.iterdir()] == ["yellow_tripdata_2015-01.parquet"]

    def test_summary_counts_downloaded_moved_and_skipped_separately(self, tmp_path, monkeypatch, capsys):
        # The summary print is what makes idempotency *observable* in job logs
        # (this was added specifically because "it already works" isn't
        # verifiable from a re-run without it) — so the three counters must be
        # attributed to the right bucket, not just summed together.
        monkeypatch.setattr(main, "START_YEAR_MONTH", (2015, 1))
        monkeypatch.setattr(main, "END_YEAR_MONTH", (2015, 3))
        self._patch_urlopen(monkeypatch)

        # 2015-01 already landed (skip); 2015-02 sits flat (move); 2015-03 missing (download).
        era_dir = tmp_path / "modern"
        era_dir.mkdir()
        (era_dir / "yellow_tripdata_2015-01.parquet").write_bytes(b"x")
        tmp_path.joinpath("yellow_tripdata_2015-02.parquet").write_bytes(b"x")

        main._land_yellow_tripdata(str(tmp_path))

        out = capsys.readouterr().out
        assert "Landed 1 new file(s)" in out
        assert "moved 1 from the old flat layout" in out
        assert "skipped 1 already present" in out
        assert "(3 total)" in out
