import unittest

from modules import period_inventory as inv

C = inv.CLEANING


class OneClickTest(unittest.TestCase):
    def test_expands_all_steps_when_nothing_done(self):
        sel, skipped = inv.expand_one_click([("台北", C, inv.ONE_CLICK)], {})
        self.assertEqual([f for _, _, f in sel], list(inv.ONE_CLICK_STEPS))
        self.assertNotIn("⑧ 06季獎金", [f for _, _, f in sel])
        self.assertEqual(skipped, {})

    def test_skips_done_steps_and_resumes_from_failed(self):
        status = {
            ("台北", C, "① 前置作業"): inv.Status(inv.DONE),
            ("台北", C, "② 00調薪"): inv.Status(inv.DONE),
            ("台北", C, "③ 01專員請款"): inv.Status(inv.DONE),
            ("台北", C, "④ 02儲值獎金"): inv.Status(inv.FAILED),
        }
        sel, skipped = inv.expand_one_click([("台北", C, inv.ONE_CLICK)], status)
        self.assertEqual(skipped["台北"], ["① 前置作業", "② 00調薪", "③ 01專員請款"])
        self.assertEqual(sel[0][2], "④ 02儲值獎金")

    def test_per_area_and_other_items_kept(self):
        status = {("台北", C, "① 前置作業"): inv.Status(inv.DONE)}
        sel, _ = inv.expand_one_click(
            [("台北", C, inv.ONE_CLICK), ("台中", C, inv.ONE_CLICK), ("台北", C, "⑫ 場次時數")], status)
        self.assertNotIn(("台北", C, "① 前置作業"), sel)
        self.assertIn(("台中", C, "① 前置作業"), sel)
        self.assertIn(("台北", C, "⑫ 場次時數"), sel)

    def test_no_duplicates_when_step_also_ticked(self):
        sel, _ = inv.expand_one_click([("台北", C, inv.ONE_CLICK), ("台北", C, "② 00調薪")], {})
        self.assertEqual(len(sel), len(set(sel)))

    def test_queue_order_follows_function_map(self):
        fmap = {C: ["① 前置作業", "② 00調薪", "③ 01專員請款", "④ 02儲值獎金", "⑤ 03新人實境",
                    "⑥ 04新人實習", "⑦ 05組長津貼", "⑧ 06季獎金", "⑨ 結算作業", "⑩ 一鍵執行"]}
        sel, _ = inv.expand_one_click([("台北", C, inv.ONE_CLICK)], {})
        q = inv.build_queue(sel, ["台北"], fmap)
        self.assertEqual([i["function"] for i in q], list(inv.ONE_CLICK_STEPS))

    def test_inventory_panel_points_to_run_settings(self):
        # 盤點表不展開一鍵執行，請使用者在上方執行設定單獨勾選
        self.assertIn(inv.ONE_CLICK, inv.NOT_BATCHABLE)


if __name__ == "__main__":
    unittest.main()
