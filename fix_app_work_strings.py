from pathlib import Path
path = Path(r'D:/codex/app_work.py')
text = path.read_text(encoding='utf-8')
text = text.replace('        text = self._replace_subsection_in_chapter(text, "五", "四", "四、审计基金财产的会计师事务所\n【待填写】")\n', '        text = self._replace_subsection_in_chapter(text, "五", "四", "四、审计基金财产的会计师事务所\\n【待填写】")\n')
path.write_text(text, encoding='utf-8')
