from backend.roadmap_engine.storage.schema import init_roadmap_schema


def main() -> None:
    init_roadmap_schema()
    print("Roadmap schema initialized.")


if __name__ == "__main__":
    main()

