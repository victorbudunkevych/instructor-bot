```markdown
# Import DB from Excel - feature/import-db-ability

This branch adds standalone import helpers and a Telegram handler to parse the project's exported Excel files and import data into the SQLite DB.

Files added:
- database_imports.py  -- DB helper functions import_instructors/import_students/import_lessons
- import_excel_handler.py -- Telegram handler import_from_excel that parses .xlsx and calls DB helpers
- IMPORT_README.md -- this file with integration instructions

Integration notes:
1. To enable the handler in bot.py, import the handler and register the command handler:

```python
from import_excel_handler import import_from_excel
application.add_handler(CommandHandler("import_excel", import_from_excel))
```

2. The handler uses `is_admin(user_id)` function defined in bot.py to check permissions. Ensure bot.is_admin is available.
3. The default mode is 'merge' (INSERT OR REPLACE). To clear existing tables before import use the command argument `clear`:
   `/import_excel clear` and attach the .xlsx file.
4. Test on a copy of the DB before using in production.
```