from pipeline.llm.llm_extractor import extract_opportunity_with_llm

sample_text = """
Google is hiring a Software Engineering Intern.

Location: India

Minimum qualifications:
- Python
- Data Structures and Algorithms

Apply by June 30, 2026
"""

result = extract_opportunity_with_llm(sample_text)

print(result)
