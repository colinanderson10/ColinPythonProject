import sys

import numpy
import pandas
import plotly.express
import plotly.subplots
import plotly.tools
import sklearn.metrics
import statsmodels.api
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor


def main():
    pandas.set_option("display.max_columns", None)

    # Input file location of csv in LOCATION and COLUMN NAMES under COLUMN_NAMES
    data = pandas.read_csv("LOCATION", header=None)
    column_names = [
        "Column1",
        "Column2",
    ]
    data.columns = column_names

    columns = data.columns.values.tolist()
    print("Column names:\n", columns)

    def entry(y):
        if y == "response":
            response = input("\nEnter response variable:\n")
            if response in columns:
                return response
            else:
                return entry("response")
        elif y == "predictor":
            predictor = input("\nEnter predictor variable(s):\n").split(", ")
            for x in predictor:
                if x in columns:
                    continue
                else:
                    return entry("predictor")
            return predictor

    response = entry("response")

    predictor = entry("predictor")

    response_feature = data[response].to_frame()
    response_number_values = response_feature.nunique()

    if response_number_values[0] < 3:
        response_type = "Categorical"
        print("The Response variable, " + response + ", is a Categorical Variable")
    else:
        response_type = "Continuous"
        print("The Response variable, " + response + ", is a Continuous Variable")

    predictor_columns = data[data.columns.intersection(predictor)]

    resp_mean = response_feature.mean()

    fulldata = response_feature

    # Build preliminary results table
    results_cols = [
        "Response",
        "Predictor",
        "Predictor Type",
        "t Score",
        "p Value",
        "Diff Mean of Response (Unweighted)",
        "Diff Mean of Response (Weighted)",
    ]
    results = pandas.DataFrame(columns=results_cols)

    for predictor in predictor_columns.iteritems():

        numpy.random.seed(seed=7878)

        predictor_data = pandas.DataFrame(predictor[1])
        fulldata = pandas.concat([fulldata, predictor_data], axis=1)
        predictor_number_values = predictor_data.nunique()
        predictor_name = predictor[0]
        if predictor_number_values[0] < 3:
            predictor_type = "Categorical"
            print(
                "The Predictor variable, "
                + predictor_name
                + ", is a Categorical Variable"
            )

        else:
            predictor_type = "Continuous"
            print(
                "The Predictor variable, "
                + predictor_name
                + ", is a Continuous Variable"
            )

        # Relationship plot
        if response_type == "Categorical" and predictor_type == "Categorical":

            conf_matrix = sklearn.metrics.confusion_matrix(
                response_feature, predictor_data
            )

            figure1 = plotly.graph_objs.Figure(
                data=plotly.graph_objs.Heatmap(
                    z=conf_matrix, zmin=0, zmax=conf_matrix.max()
                )
            )
            figure1.update_layout(
                title="Categorical Predictor by Categorical Response (without relationship)",
                xaxis_title=response,
                yaxis_title=predictor_name,
            )
            figure1.show()

            response_html = response.replace(" ", "")
            predictor_name_html = predictor_name.replace(" ", "")
            relate_file_save = f"{response_html}_{predictor_name_html}_heat_map.html"
            figure1.write_html(file=relate_file_save, include_plotlyjs="cdn")

        elif response_type == "Categorical" and predictor_type == "Continuous":

            n = response_feature.shape[0]

            binomialvalues = response_feature[response].unique().tolist()

            if binomialvalues[0] > binomialvalues[1]:
                high = binomialvalues[0]
                low = binomialvalues[1]
            else:
                high = binomialvalues[1]
                low = binomialvalues[0]

            highvalues = data[predictor_name].groupby(data[response]).apply(list)[high]
            lowvalues = data[predictor_name].groupby(data[response]).apply(list)[low]

            hist_data = [highvalues, lowvalues]

            group_labels = ["Response = " + str(high), "Response = " + str(low)]

            binsize = input(
                f"\nEnter size of the bins for the Distribution Plot for predictor variable {predictor_name}:\n"
            )
            DistributionPlot = plotly.tools.FigureFactory.create_distplot(
                hist_data, group_labels, bin_size=binsize
            )
            DistributionPlot.update_layout(
                title="Continuous Predictor by Categorical Response",
                xaxis_title=predictor_name,
                yaxis_title="Distribution",
            )
            DistributionPlot.show()

            response_html = response.replace(" ", "")
            predictor_name_html = predictor_name.replace(" ", "")
            relate_file_save = (
                f"{response_html}_{predictor_name_html}_distribution_plot.html"
            )
            DistributionPlot.write_html(file=relate_file_save, include_plotlyjs="cdn")

            figure1 = plotly.graph_objs.Figure()
            for curr_hist, curr_group in zip(hist_data, group_labels):
                figure1.add_trace(
                    plotly.graph_objs.Violin(
                        x=numpy.repeat(curr_group, n),
                        y=curr_hist,
                        name=curr_group,
                        box_visible=True,
                        meanline_visible=True,
                    )
                )
            figure1.update_layout(
                title="Continuous Predictor by Categorical Response",
                xaxis_title=response,
                yaxis_title=predictor_name,
            )
            figure1.show()

            response_html = response.replace(" ", "")
            predictor_name_html = predictor_name.replace(" ", "")
            relate_file_save = f"{response_html}_{predictor_name_html}_violin_plot.html"
            figure1.write_html(file=relate_file_save, include_plotlyjs="cdn")

        elif response_type == "Continuous" and predictor_type == "Categorical":

            n = response_feature.shape[0]

            binomialvalues = predictor_data[predictor_name].unique().tolist()

            if binomialvalues[0] > binomialvalues[1]:
                high = binomialvalues[0]
                low = binomialvalues[1]
            else:
                high = binomialvalues[1]
                low = binomialvalues[0]

            highvalues = data[response].groupby(data[predictor_name]).apply(list)[high]
            lowvalues = data[response].groupby(data[predictor_name]).apply(list)[low]

            hist_data = [highvalues, lowvalues]

            group_labels = ["Group 1", "Group 2"]

            binsize = input(
                f"\nEnter size of the bins for the Distribution Plot for response variable {response}:\n"
            )
            DistributionPlot = plotly.tools.FigureFactory.create_distplot(
                hist_data, group_labels, bin_size=float(binsize)
            )

            DistributionPlot.update_layout(
                title="Continuous Response by Categorical Predictor",
                xaxis_title=response,
                yaxis_title="Distribution",
            )
            DistributionPlot.show()

            response_html = response.replace(" ", "")
            predictor_name_html = predictor_name.replace(" ", "")
            relate_file_save = (
                f"{response_html}_{predictor_name_html}_distribution_plot.html"
            )
            DistributionPlot.write_html(file=relate_file_save, include_plotlyjs="cdn")

            figure1 = plotly.graph_objs.Figure()
            for curr_hist, curr_group in zip(hist_data, group_labels):
                figure1.add_trace(
                    plotly.graph_objs.Violin(
                        x=numpy.repeat(curr_group, n),
                        y=curr_hist,
                        name=curr_group,
                        box_visible=True,
                        meanline_visible=True,
                    )
                )
            figure1.update_layout(
                title="Continuous Response by Categorical Predictor",
                xaxis_title="Groupings",
                yaxis_title=response,
            )
            figure1.show()

            response_html = response.replace(" ", "")
            predictor_name_html = predictor_name.replace(" ", "")
            relate_file_save = f"{response_html}_{predictor_name_html}_violin_plot.html"
            figure1.write_html(file=relate_file_save, include_plotlyjs="cdn")

        elif response_type == "Continuous" and predictor_type == "Continuous":

            figure1 = plotly.express.scatter(
                x=predictor_data[predictor_name],
                y=response_feature[response],
                trendline="ols",
            )
            figure1.update_layout(
                title="Continuous Response by Continuous Predictor",
                xaxis_title=predictor_name,
                yaxis_title=response,
            )
            figure1.show()

            response_html = response.replace(" ", "")
            predictor_name_html = predictor_name.replace(" ", "")
            relate_file_save = (
                f"{response_html}_{predictor_name_html}_scatter_plot.html"
            )
            figure1.write_html(file=relate_file_save, include_plotlyjs="cdn")

        # Regression
        if response_type == "Categorical":
            reg_model = statsmodels.api.Logit(
                response_feature[response], predictor_data[predictor_name]
            )

        else:
            reg_model = statsmodels.api.OLS(
                response_feature[response], predictor_data[predictor_name]
            )

        # Fit model
        fitted_model = reg_model.fit()

        # Get t val and p score
        t_score = round(fitted_model.tvalues[0], 6)
        p_value = "{:.6e}".format(fitted_model.pvalues[0])

        # Plot regression
        reg_fig = plotly.express.scatter(
            y=response_feature[response],
            x=predictor_data[predictor_name],
            trendline="ols",
        )

        reg_fig.update_layout(
            title=f"Regression: {response} on {predictor_name}",
            xaxis_title=predictor_name,
            yaxis_title=response,
        )

        reg_fig.show()

        reg_file_save = f"{response_html}_{predictor_name_html}_reg.html"
        reg_fig.write_html(file=reg_file_save, include_plotlyjs="cdn")

        # create difference with mean of response table
        n = response_feature.shape[0]

        # get number of bins
        if predictor_type == "Continuous":
            print(
                f"\nChoose the number of bins for the difference mean with response for predictor variable {predictor_name}"
            )
            bins = int(input("\nEnter number of bins:\n"))
        else:
            bins = 2
        min = data[predictor_name].min()
        max = data[predictor_name].max()
        range = max - min
        binsize2 = range / bins
        LowerBin = []
        UpperBin = []
        BinCenters = []
        BinCount = []
        BinMeans = []
        PopulationMean = []
        MeanSquareDiff = []
        PopulationProportion = []
        MeanSquareDiffWeighted = []
        i = 0
        t = 0
        a = min
        b = min + binsize2
        c = min + (binsize2 / 2)
        while i < bins:
            LowerBin.append(a)
            a += binsize2
            UpperBin.append(b)
            b += binsize2
            BinCenters.append(c)
            c += binsize2
            i += 1
        while t < bins:
            BinCount.append(
                data.loc[
                    data[predictor_name].between(
                        LowerBin[t], UpperBin[t], inclusive=True
                    ),
                    response,
                ].count()
            )
            BinMeans.append(
                data.loc[
                    data[predictor_name].between(
                        LowerBin[t], UpperBin[t], inclusive=True
                    ),
                    response,
                ].mean()
            )
            PopulationMean.append(resp_mean[0])
            MeanSquareDiff.append((BinMeans[t] - PopulationMean[t]) ** 2)
            PopulationProportion.append(BinCount[t] / n)
            MeanSquareDiffWeighted.append(MeanSquareDiff[t] * PopulationProportion[t])
            t += 1
        msdtable = pandas.DataFrame(
            list(
                zip(
                    LowerBin,
                    UpperBin,
                    BinCenters,
                    BinCount,
                    BinMeans,
                    PopulationMean,
                    MeanSquareDiff,
                    PopulationProportion,
                    MeanSquareDiffWeighted,
                )
            ),
            columns=[
                "LowerBin",
                "UpperBin",
                "BinCenters",
                "BinCount",
                "BinMeans",
                "PopulationMean",
                "MeanSquareDiff",
                "PopulationProportion",
                "MeanSquareDiffWeighted",
            ],
        )
        msd_uw = (msdtable["MeanSquareDiff"].sum()) / bins
        msd_w = msdtable["MeanSquareDiffWeighted"].sum()
        print(msdtable)

        msdtable.to_html(f"{response_html}_{predictor_name_html}_diff_table.html")

        # create difference with mean of response plot
        fig_diff = plotly.subplots.make_subplots(specs=[[{"secondary_y": True}]])
        fig_diff.add_trace(
            plotly.graph_objs.Bar(
                x=BinCenters,
                y=BinCount,
                name="Observations",
            )
        )
        fig_diff.add_trace(
            plotly.graph_objs.Scatter(
                x=BinCenters,
                y=BinMeans,
                line=dict(color="red"),
                name="Bin Mean",
            ),
            secondary_y=True,
        )

        fig_diff.add_trace(
            plotly.graph_objs.Scatter(
                x=BinCenters,
                y=PopulationMean,
                line=dict(color="green"),
                name="Population Mean",
            ),
            secondary_y=True,
        )

        fig_diff.update_layout(
            title_text=f"Difference in Mean Response: {response} and {predictor_name}",
        )
        fig_diff.update_xaxes(title_text=f"{predictor_name} (binned)")
        fig_diff.update_yaxes(title_text="count", secondary_y=False)
        fig_diff.update_yaxes(title_text=f"{response}", secondary_y=True)

        fig_diff.show()

        fig_diff_file_save = f"{response_html}_{predictor_name_html}_diff.html"
        fig_diff.write_html(file=fig_diff_file_save, include_plotlyjs="cdn")

        # Add values to results table
        results = results.append(
            {
                "Response": response,
                "Predictor": predictor_name,
                "Predictor Type": predictor_type,
                "t Score": t_score,
                "p Value": p_value,
                "Diff Mean of Response (Unweighted)": msd_uw,
                "Diff Mean of Response (Weighted)": msd_w,
            },
            ignore_index=True,
        )

    # get random forest importance values for each predictor
    if response_type == "Categorical":
        model = RandomForestClassifier()
    else:
        model = RandomForestRegressor()

    model.fit(fulldata.iloc[:, 1:], fulldata.iloc[:, 0])
    importance = model.feature_importances_
    importancedataframe = pandas.DataFrame(columns=["Random Forest Importance"])
    importancedataframe["Random Forest Importance"] = importance

    # append importance values to table of results
    results = pandas.concat([results, importancedataframe], axis=1)

    results.to_html("Results Table.html")


if __name__ == "__main__":
    sys.exit(main())
