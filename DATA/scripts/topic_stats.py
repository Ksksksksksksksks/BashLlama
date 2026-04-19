import pandas as pd
import matplotlib.pyplot as plt
import sys
import os


def analyze_and_clean(csv_path, output_dir='../corpus', output_file='topics_cleaned.csv'):
    # Load with encoding auto-detection (fallback to utf-8-sig)
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, encoding='cp1251')
    print(f"Loaded {len(df)} rows from {csv_path}")

    # Keep only rows with non-empty topic
    df = df[df['topic'].notna() & (df['topic'].str.strip() != '')].copy()
    print(f"After dropping empty topics: {len(df)} rows")

    # Initial statistics
    counts = df['topic'].value_counts()
    total = len(df)
    unique = len(counts)
    print(f"\n=== Initial statistics ===")
    print(f"Total titles: {total}")
    print(f"Unique topics: {unique}")
    print("\nDistribution:")
    for topic, cnt in counts.items():
        print(f"  {topic}: {cnt} ({cnt / total * 100:.1f}%)")

    # Compute helpful thresholds
    min_count = counts.min()
    max_count = counts.max()
    mean_count = counts.mean()
    median_count = counts.median()
    print(f"\n=== Category size statistics ===")
    print(f"Min: {min_count}, Max: {max_count}, Mean: {mean_count:.2f}, Median: {median_count:.1f}")

    # Ask user for threshold
    while True:
        try:
            thresh = input("\nEnter minimum number of samples per category to keep: ").strip()
            if thresh == '':
                thresh = 3  # default
                print(f"Using default threshold = {thresh}")
                break
            thresh = int(thresh)
            if thresh < 1:
                print("Threshold must be >= 1")
                continue
            break
        except ValueError:
            print("Please enter a valid integer.")

    # Filter categories below threshold
    keep_topics = counts[counts >= thresh].index.tolist()
    removed_topics = counts[counts < thresh].index.tolist()
    df_clean = df[df['topic'].isin(keep_topics)].copy()
    print(f"\nKeeping topics: {keep_topics}")
    print(f"Removing topics (size < {thresh}): {removed_topics}")

    # New statistics after cleaning
    clean_counts = df_clean['topic'].value_counts()
    clean_total = len(df_clean)
    print(f"\n=== After cleaning ===")
    print(
        f"Removed {total - clean_total} rows ({total - clean_total}/{total} = {(total - clean_total) / total * 100:.1f}%)")
    print(f"Remaining unique topics: {len(clean_counts)}")
    print("\nNew distribution:")
    for topic, cnt in clean_counts.items():
        print(f"  {topic}: {cnt} ({cnt / clean_total * 100:.1f}%)")

    # Save cleaned CSV
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, output_file)
    df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\nCleaned dataset saved to: {output_path}")

    # Plot before/after comparison
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Before plot
    counts_sorted = counts.sort_values(ascending=False)
    axes[0].bar(range(len(counts_sorted)), counts_sorted.values, color='skyblue', edgecolor='black')
    axes[0].set_xticks(range(len(counts_sorted)))
    axes[0].set_xticklabels(counts_sorted.index, rotation=45, ha='right', fontsize=8)
    axes[0].set_title(f'Before cleaning ({total} titles, {unique} topics)')
    axes[0].set_xlabel('Topic')
    axes[0].set_ylabel('Number of titles')

    # After plot
    clean_counts_sorted = clean_counts.sort_values(ascending=False)
    axes[1].bar(range(len(clean_counts_sorted)), clean_counts_sorted.values, color='lightgreen', edgecolor='black')
    axes[1].set_xticks(range(len(clean_counts_sorted)))
    axes[1].set_xticklabels(clean_counts_sorted.index, rotation=45, ha='right', fontsize=8)
    axes[1].set_title(f'After cleaning (threshold ≥ {thresh})')
    axes[1].set_xlabel('Topic')
    axes[1].set_ylabel('Number of titles')

    plt.tight_layout()
    plot_path = os.path.join(output_dir, 'topic_comparison.png')
    plt.savefig(plot_path, dpi=150)
    plt.show()
    print(f"Comparison chart saved to: {plot_path}")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = input("CSV-path: ").strip()
    analyze_and_clean(csv_file)