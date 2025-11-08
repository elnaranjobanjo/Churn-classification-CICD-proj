from src.train import train_and_validate


def main():
    _, metrics = train_and_validate()
    print("Validation metrics:")
    for name, value in metrics.items():
        print(f"  {name}: {value:.4f}")


if __name__ == "__main__":
    main()
