import ast
from backend.roadmap_engine.storage.database import get_connection

conn = get_connection()
cursor = conn.cursor()

cursor.execute("""
SELECT title, company, type, deadline, skills, url, last_updated
FROM opportunities
""")

rows = cursor.fetchall()

print("\n📌 STORED OPPORTUNITIES\n")
print("=" * 80)

for row in rows:
    title, company, type_, deadline, skills, url, last_updated = row

    # Convert skills string → real list
    try:
        skills = ast.literal_eval(skills)
    except:
        skills = []

    print(f"🏢 Company      : {company}")
    print(f"🎯 Title        : {title}")
    print(f"📌 Type         : {type_}")
    print(f"⏳ Deadline     : {deadline if deadline else 'Not specified'}")
    print(f"🧠 Skills       : {', '.join(skills) if skills else 'Not extracted'}")
    print(f"🔗 URL          : {url}")
    print(f"🕒 Last Updated : {last_updated}")
    print("-" * 80)

conn.close()
