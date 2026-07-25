# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for ``batch_service.scheduler._schedule_tick``.

The load-bearing property: each operational section (extract / map /
materialize) is dispatched independently. A previous version used a
single loop with early ``continue`` statements, which silently gated
materialize on mappers being due — on iagai this caused materialize to
stop firing entirely for 8 days after mappers ran on a different cron
slot to entities. The refactor splits each section into its own helper
so a skip in one cannot leak into another.
"""

from unittest.mock import patch

from batch_service import scheduler


class TestTickIndependence:
    def test_materialize_still_dispatched_when_mappers_not_due(self, tmp_path):
        """The regression case: mappers_due=[] must NOT prevent
        materialize from running. This was the 8-day iagai stall."""
        model_name = "test_model"
        model_dir = tmp_path / model_name
        model_dir.mkdir()

        with (
            patch("core.registry.load_registry", return_value={model_name: {}}),
            patch("batch.schedule.extractors_due", return_value=[]),
            patch("batch.schedule.mappers_due", return_value=[]),
            patch("batch.schedule.entities_due", return_value=["some_entity"]),
            patch("batch_service.scheduler._any_writer_running",
                  return_value=(False, None)),
            patch("batch_service.executor.submit_materialize") as sub_mat,
            patch("batch_service.executor.submit_mapper") as sub_map,
            patch("batch_service.executor.submit") as sub_ext,
            patch("batch_service.db.get_connection") as get_conn,
            patch("batch_service.db.insert_job"),
        ):
            get_conn.return_value.__enter__.return_value = None
            scheduler._schedule_tick(tmp_path)

            # Materialize should have been submitted despite empty mapper list
            assert sub_mat.called, "materialize was silently skipped when mappers weren't due"
            assert not sub_map.called
            assert not sub_ext.called

    def test_mappers_still_dispatched_when_extractors_not_due(self, tmp_path):
        """Symmetric case: no extractors due must not gate mappers."""
        model_name = "test_model"
        model_dir = tmp_path / model_name
        model_dir.mkdir()

        with (
            patch("core.registry.load_registry", return_value={model_name: {}}),
            patch("batch.schedule.extractors_due", return_value=[]),
            patch("batch.schedule.mappers_due", return_value=["some_mapper"]),
            patch("batch.schedule.entities_due", return_value=[]),
            patch("batch_service.scheduler._any_writer_running",
                  return_value=(False, None)),
            patch("batch_service.executor.submit_materialize") as sub_mat,
            patch("batch_service.executor.submit_mapper") as sub_map,
            patch("batch_service.db.get_connection") as get_conn,
            patch("batch_service.db.insert_job"),
        ):
            get_conn.return_value.__enter__.return_value = None
            scheduler._schedule_tick(tmp_path)

            assert sub_map.called
            assert not sub_mat.called

    def test_all_three_sections_fire_when_all_due(self, tmp_path):
        model_name = "test_model"
        model_dir = tmp_path / model_name
        model_dir.mkdir()

        with (
            patch("core.registry.load_registry", return_value={model_name: {}}),
            patch("batch.schedule.extractors_due", return_value=["git"]),
            patch("batch.schedule.mappers_due", return_value=["team"]),
            patch("batch.schedule.entities_due", return_value=["Repository"]),
            patch("batch_service.scheduler._any_writer_running",
                  return_value=(False, None)),
            patch("batch_service.executor.submit_materialize") as sub_mat,
            patch("batch_service.executor.submit_mapper") as sub_map,
            patch("batch_service.executor.submit") as sub_ext,
            patch("batch_service.db.get_connection") as get_conn,
            patch("batch_service.db.insert_job"),
        ):
            get_conn.return_value.__enter__.return_value = None
            scheduler._schedule_tick(tmp_path)

            assert sub_ext.called
            assert sub_map.called
            assert sub_mat.called

    def test_writer_busy_on_mapper_does_not_gate_materialize(self, tmp_path):
        """When another writer holds the lock at the mapper stage, the
        old code returned via outer-loop ``continue`` — skipping
        materialize even though the tick was independent. The refactor
        makes each section handle its own busy-check locally."""
        model_name = "test_model"
        model_dir = tmp_path / model_name
        model_dir.mkdir()

        # First busy-check (for mapper) says busy, second (for materialize)
        # says free — simulates a mapper-run that finished between the two
        # section calls. Materialize should still fire.
        with (
            patch("core.registry.load_registry", return_value={model_name: {}}),
            patch("batch.schedule.extractors_due", return_value=[]),
            patch("batch.schedule.mappers_due", return_value=["team"]),
            patch("batch.schedule.entities_due", return_value=["Repository"]),
            patch("batch_service.scheduler._any_writer_running",
                  side_effect=[(True, "job-x"), (False, None)]),
            patch("batch_service.executor.submit_materialize") as sub_mat,
            patch("batch_service.executor.submit_mapper") as sub_map,
            patch("batch_service.db.get_connection") as get_conn,
            patch("batch_service.db.insert_job"),
        ):
            get_conn.return_value.__enter__.return_value = None
            scheduler._schedule_tick(tmp_path)

            # Mapper skipped (busy), materialize still fired
            assert not sub_map.called
            assert sub_mat.called
