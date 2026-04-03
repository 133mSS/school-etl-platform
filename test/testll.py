
extractor = SimpleExtractor(
    "postgresql://school_user:school_pass@localhost:5434/school_source"
)
df = extractor.read_table("sinh_vien")
print(df.head())