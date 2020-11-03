import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

    
def plot_func(df):
    
    summary_df = pd.DataFrame(df.groupby(by = ["Organisation Previous NZTE Segment", "SnapshotNZDate"])["Organisation Key"].nunique()
                    .reset_index(name = "customer_count")
                    .pivot(index = "Organisation Previous NZTE Segment", columns=  "SnapshotNZDate", values = "customer_count")
                    .fillna(0)
    )    
    #plotting heatmap
    plt.figure(figsize = (10, 5))

    sns.set(font_scale = 1) 
    sns.heatmap(summary_df, annot=True,  annot_kws={"size": 10} #configure font
                , linewidth=.1, vmax=99, fmt='.0f', cmap='YlOrRd', square=True, cbar=False)

    plt.xlabel("snapshot date")
    plt.ylabel("Prev Segment")

    return summary_df, plt