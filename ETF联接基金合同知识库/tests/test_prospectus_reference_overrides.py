import unittest

from app import ProspectusEngine


class ProspectusReferenceOverridesTests(unittest.TestCase):
    def test_reference_override_texts_are_loaded_from_clause_library(self):
        engine = ProspectusEngine()
        self.assertIn("PROSPECTUS_REFERENCE_REPLACEMENTS", engine.clauses)
        self.assertIn("PRODUCT_SUMMARY_DISCLOSURE_WORDING", engine.clauses)

    def test_rewrites_isolated_side_pocket_valuation_source(self):
        source = "\n".join(
            [
                "一、估值日",
                "估值日正文。",
                "十、实施侧袋机制期间的基金资产估值",
                "本基金实施侧袋机制的，应根据本部分的约定对主袋账户资产进行估值并披露主袋账户的基金净值信息，暂停披露侧袋账户份额净值。",
            ]
        )

        result = ProspectusEngine._apply_prospectus_reference_overrides(source)

        self.assertIn("十、实施侧袋机制期间的基金资产估值", result)
        self.assertIn("本基金实施侧袋机制的，本基金的估值安排详见招募说明书“侧袋机制”部分的规定。", result)
        self.assertNotIn("应根据本部分的约定对主袋账户资产进行估值", result)
        self.assertNotIn("暂停披露侧袋账户份额净值", result)

    def test_rewrites_side_pocket_information_disclosure_reference(self):
        source = "\n".join(
            [
                "（十七）实施侧袋机制期间的信息披露",
                "本基金实施侧袋机制的，相关信息披露义务人应当根据法律法规、基金合同和招募说明书的规定进行信息披露，详见招募说明书的规定。",
            ]
        )

        result = ProspectusEngine._apply_prospectus_reference_overrides(source)

        self.assertIn(
            "本基金实施侧袋机制的，相关信息披露义务人应当根据法律法规、基金合同和招募说明书的规定进行信息披露，详见招募说明书“侧袋机制”部分的规定。",
            result,
        )
        self.assertNotIn("详见招募说明书的规定。", result)


if __name__ == "__main__":
    unittest.main()
