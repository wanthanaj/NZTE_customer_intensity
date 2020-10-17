import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# Folder that hold outputs
outDir  = 'C://Users//Wanthana.J//AzureML//Netflix//customerIntensity//data//'

# All FOCUS
focus_snapshot= pd.read_csv(outDir + 'focus_snapshot20200910.csv') #, parse_dates= ["Date"] , date_parser= mydateparser)

#replace Focus 700 with Focus
focus_snapshot['Organisation Previous NZTE Segment'] = focus_snapshot['Organisation Previous NZTE Segment'].replace(['Focus 700'],'Focus')
focus_snapshot['Organisation NZTE Segment'] = focus_snapshot['Organisation Previous NZTE Segment'].replace(['Focus 700'],'Focus')

from custom_pnt import plot_func

df, hm = plot_func(focus_snapshot)

print(df)

hm.show()
