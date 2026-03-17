from pathlib import Path
path = Path(r'D:/codex/01_招募说明书模板_work.md')
text = path.read_text(encoding='utf-8')
start = text.rindex('第四章  基金托管人')
end = text.index('第五章  相关服务机构', start)
text = text[:start] + '第四章  基金托管人\n【托管人情况待填写】\n' + text[end:]
start = text.rindex('第二十一章  基金托管协议的内容摘要')
end = text.index('第二十二章  基金份额持有人服务', start)
text = text[:start] + '第二十一章  基金托管协议的内容摘要\n一、基金托管协议当事人\n二、基金托管人对基金管理人的业务监督和核查\n三、基金管理人对基金托管人的业务核查\n四、基金财产的保管\n五、基金资产净值计算和会计核算\n六、基金份额持有人名册的保管\n七、适用法律与争议解决方式\n八、基金托管协议的变更、终止与基金财产的清算\n' + text[end:]
path.write_text(text, encoding='utf-8')
