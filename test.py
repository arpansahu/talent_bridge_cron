import spacy

# Load the spaCy model
nlp = spacy.load("en_core_web_sm")

# Job description text
job_description = """
Minimum qualifications:
Bachelor’s degree in Computer Science, a related field, or equivalent practical experience.
8 years of experience with data structures or algorithms.
5 years of experience with software development in one or more programming languages.
3 years of people management experience, and experience designing, analyzing, and troubleshooting distributed systems.
"""

# Process the text with spaCy
doc = nlp(job_description)

# Extract years of experience related to software development
software_dev_experience = 0
for sent in doc.sents:
    if "software development" in sent.text.lower():
        for token in sent:
            if token.like_num:
                software_dev_experience = int(token.text)
                break

print(f"Years of experience required for software development: {software_dev_experience} years")