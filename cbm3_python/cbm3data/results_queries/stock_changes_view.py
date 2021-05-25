import pandas as pd


def get_stock_changes_view(tfi):
    df = pd.DataFrame(index=tfi.index)

    df["Delta Total Ecosystem"] = \
        tfi.GrossGrowth_AG+tfi.GrossGrowth_BG - \
        (tfi.BioCO2Emission + tfi.BioCH4Emission +
         tfi.BioCOEmission + tfi.DOMCO2Emission +
         tfi.DOMCH4Emssion + tfi.DOMCOEmission +
         tfi.SoftProduction + tfi.HardProduction +
         tfi.DOMProduction)

    return df
