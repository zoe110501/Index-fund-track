import unittest

from app import ContractEngine


class ContractSummaryGenerationTests(unittest.TestCase):
    def test_contract_summary_rewrites_quoted_top_level_heading_references(self):
        source = "\n".join(
            [
                "第十五部分  基金费用与税收",
                "一、基金费用的种类",
                "费用种类正文。",
                "二、基金费用计提方法、计提标准和支付方式",
                "上述“一、基金费用的种类”中第4-12项费用，根据有关法规及相应协议规定，按费用实际支出金额列入当期费用。",
                "第二十四部分  基金合同内容摘要",
                "四、基金费用与税收",
                "{CONTRACT_SUMMARY_FEES}",
            ]
        )

        rendered = ContractEngine()._replace_contract_summary_placeholders(source)

        summary = rendered.split("第二十四部分  基金合同内容摘要", 1)[1]

        self.assertIn("上述“（一）基金费用的种类”中第4-12项费用", summary)
        self.assertNotIn("上述“一、基金费用的种类”", summary)


if __name__ == "__main__":
    unittest.main()
