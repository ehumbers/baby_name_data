import os
import pandas as pd


def process_raw():
    dfs = []
    for fname in os.listdir("raw"):
        if not fname.startswith("yob"):
            continue
        df = pd.read_csv(os.path.join('raw', fname), names=["name", "gender", "count"])
        year = int(fname[3:7])
        df["year"] = year
        dfs.append(df)

    # there are 725 cheynes in the world, so setting the cutoff to 724
    tall_df = pd.concat(dfs)
    counts = tall_df.groupby("name")["count"].sum().sort_values(ascending=False)
    names = counts[counts > 724].index

    tall_df[tall_df["name"].isin(names)]
    return tall_df


def pivot(df):
    pdf = (
        df.pivot_table(
            index="year", columns=["name", "gender"], values="count", aggfunc=sum
        )
        .fillna(0)
        .astype(int)
    )
    return pdf

def save_data(pdf):
    names = set(pdf.columns.get_level_values(0))
    for name in names:
        df = pdf[name]
        for sex in ['M', 'F']:
            if sex not in df.columns:
                df[sex] = 0

        first_letter = name[0].upper()
        data_folder = 'processed'
        to_save_folder = os.path.join(data_folder, first_letter)
        if not os.path.exists(to_save_folder):
            os.makedirs(to_save_folder)

        df.columns = ['Boys', 'Girls']
        to_save = os.path.join(to_save_folder, name.lower() + '.csv')
        # df.to_csv(to_save, index=False)
        df.to_csv(to_save, index=True)



if __name__ == '__main__':
    df = process_raw()
    pdf = pivot(df)
    pdf.to_csv("names_pivoted.csv")
    save_data(pdf)




