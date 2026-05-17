import os
import tempfile
import unittest

from queue_bot import database as db


class DatabaseTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.old_db = db.DB
        db.DB = self.tmp.name
        await db.init()

    async def asyncTearDown(self):
        db.DB = self.old_db
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    async def test_category_thresholds(self):
        self.assertEqual(db.category(65), "good")
        self.assertEqual(db.category(64), "middle")
        self.assertEqual(db.category(35), "poor")

    async def test_group_filter_and_class_duration(self):
        subject_a = await db.add_subject("Теория вероятностей", "ИКБО-01-23")
        subject_b = await db.add_subject("Физика", "БСБО-02-23")
        subject_common = await db.add_subject("Военная подготовка", None)
        await db.add_class(subject_a, "2026-05-17T09:00:00", "А-403", "Осадченко", 90)
        await db.add_class(subject_b, "2026-05-17T10:40:00", "Б-201", "Петров", 80)
        await db.add_class(subject_common, "2026-05-17T12:20:00", "В-101", "Иванов", 45)

        classes = await db.classes_for_date("2026-05-17", "ИКБО-01-23")
        self.assertEqual([item["subject_name"] for item in classes], ["Теория вероятностей", "Военная подготовка"])
        self.assertEqual(db.class_end_time(classes[0]["dt"], classes[0]["duration_minutes"]).strftime("%H:%M"), "10:30")

    async def test_randomize_queue_and_mark_submission(self):
        subject_id = await db.add_subject("ТРПП", "ИКБО-01-23")
        class_id = await db.add_class(subject_id, "2026-05-17T09:00:00", "А-403", "Преподаватель", 90)
        queue = await db.queue_for_class(class_id)
        good = await db.ensure_user(1, "good", "Good Student", "ИКБО-01-23")
        middle = await db.ensure_user(2, "middle", "Middle Student", "ИКБО-01-23")
        poor = await db.ensure_user(3, "poor", "Poor Student", "ИКБО-01-23")
        await db.set_rating(good["id"], 80)
        await db.set_rating(middle["id"], 50)
        await db.set_rating(poor["id"], 20)
        await db.join_queue(queue["id"], good["id"])
        await db.join_queue(queue["id"], middle["id"])
        await db.join_queue(queue["id"], poor["id"])

        await db.randomize_queue(queue["id"])
        entries = await db.queue_entries(queue["id"])
        self.assertEqual([entry["position"] for entry in entries], [1, 2, 3])
        self.assertEqual([entry["user_cat"] for entry in entries], ["good", "middle", "poor"])

        await db.mark_submission(queue["id"], poor["id"], "no_show")
        updated = await db.get_user(poor["id"])
        self.assertEqual(updated["rating"], 10)
        self.assertEqual(updated["no_show"], 1)

    async def test_carry_queue_moves_unsubmitted_users(self):
        subject_id = await db.add_subject("ТРПП", "ИКБО-01-23")
        first_class = await db.add_class(subject_id, "2026-05-17T09:00:00", "А-403", "Преподаватель", 90)
        second_class = await db.add_class(subject_id, "2026-05-24T09:00:00", "А-403", "Преподаватель", 90)
        first_queue = await db.queue_for_class(first_class)
        student = await db.ensure_user(5, "late", "Late Student", "ИКБО-01-23")
        await db.join_queue(first_queue["id"], student["id"])

        next_queue_id = await db.carry_queue(first_queue["id"], second_class)
        carried = await db.queue_entries(next_queue_id)
        self.assertEqual([entry["user_id"] for entry in carried], [student["id"]])
        self.assertEqual((await db.get_queue(first_queue["id"]))["status"], "completed")


if __name__ == "__main__":
    unittest.main()
