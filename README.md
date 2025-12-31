# The-Drug-Database

### Importing the Database
**Imorting Schema into MySQL:**
cd into /backend/database
```
mysql -u root -p < schema.sql
```

**Importing drug records:**
cd into /backend/database
```
python3 load_data.py
```

### Running the database
**Backend:**
cd into /backend/database
```
uvicorn server:app --host 0.0.0.0 --port 8001 --reload
```

**Frontend:**
cd into /frontend
```
npm run dev
```