# from src.train import train_and_validate
from orchestration.run import run_experiment


def main():
    metrics = run_experiment()
    print("Logged metrics:")
    for key, value in metrics.items():
        print(f"  {key}: {value:.4f}")


if __name__ == "__main__":
    main()
