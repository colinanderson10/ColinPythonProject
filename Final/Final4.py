import os
import sys

import numpy
import pandas
import plotly.express
import plotly.figure_factory
import plotly.subplots
import plotly.tools
import sklearn.metrics
import statsmodels.api
from pyspark.sql import SparkSession
from sklearn.ensemble import RandomForestClassifier


def main():

    pandas.set_option("display.max_columns", None)
    # Load dataframe, get variable info
    ########################################
    path = os.getcwd()

    # Setup Spark
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # put in sql database username here
    username = "root"
    # put in sql database password here
    pword = "secret"

    # load in the batter_counts table from the baseball database.
    # Works with default port of 3306. Change url if needed
    baseballspark = (
        spark.read.format("jdbc")
        .options(
            url="jdbc:mysql://mysql:3306/baseball",
            driver="com.mysql.cj.jdbc.Driver",
            user=username,
            password=pword,
            query="SELECT  * FROM FINALSTATS",
        )
        .load()
    )

    baseball = baseballspark.toPandas()

    baseball = baseball.apply(pandas.to_numeric)

    baseball = baseball.dropna()

    # Pretending for now this is gonna be a csv or similar txt file
    columns = baseball.columns.values.tolist()

    print("\nColumn names:\n", columns)

    response = "HomeTeamWin"

    response_feature = baseball[response].to_frame()

    predictor_columns = baseball.drop(columns=["HomeTeamWin"])

    resp_mean = response_feature.mean()

    fulldata = response_feature

    results_cols = [
        "Response",
        "Predictor",
        "t Score",
        "p Value",
        "Reg Plot",
        "Diff Mean of Response (Unweighted)",
        "Diff Mean of Response (Weighted)",
        "DMR Plot",
    ]
    results = pandas.DataFrame(columns=results_cols)

    for predictor in predictor_columns.iteritems():

        numpy.random.seed(seed=7878)

        predictor_data = pandas.DataFrame(predictor[1])
        fulldata = pandas.concat([fulldata, predictor_data], axis=1)
        predictor_name = predictor[0]

        n = response_feature.shape[0]

        binomialvalues = response_feature[response].unique().tolist()

        if binomialvalues[0] > binomialvalues[1]:
            high = binomialvalues[0]
            low = binomialvalues[1]
        else:
            high = binomialvalues[1]
            low = binomialvalues[0]

        highvalues = (
            baseball[predictor_name].groupby(baseball[response]).apply(list)[high]
        )
        lowvalues = (
            baseball[predictor_name].groupby(baseball[response]).apply(list)[low]
        )

        hist_data = [highvalues, lowvalues]

        group_labels = ["Response = " + str(high), "Response = " + str(low)]

        binsize = 0.1
        DistributionPlot = plotly.tools.FigureFactory.create_distplot(
            hist_data, group_labels, bin_size=binsize
        )
        DistributionPlot.update_layout(
            title="Continuous Predictor by Categorical Response",
            xaxis_title=predictor_name,
            yaxis_title="Distribution",
        )

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

        response_html = response.replace(" ", "")
        predictor_name_html = predictor_name.replace(" ", "")
        relate_file_save = f"{response_html}_{predictor_name_html}_violin_plot.html"
        figure1.write_html(file=relate_file_save, include_plotlyjs="cdn")

        reg_model = statsmodels.api.Logit(
            response_feature[response], predictor_data[predictor_name]
        )

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

        reg_file_save = f"{path}/results/{response_html}_{predictor_name_html}_reg.html"
        reg_fig.write_html(file=reg_file_save, include_plotlyjs="cdn")

        reg_file_link = (
            "<a target='blank' href="
            + f"{response_html}_{predictor_name_html}_reg.html"
            + ">PLOT</a>"
        )

        # create difference with mean of response table
        n = response_feature.shape[0]

        # get number of bins

        bins = 10

        min = baseball[predictor_name].min()
        max = baseball[predictor_name].max()
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
                baseball.loc[
                    baseball[predictor_name].between(
                        LowerBin[t], UpperBin[t], inclusive=True
                    ),
                    response,
                ].count()
            )
            BinMeans.append(
                baseball.loc[
                    baseball[predictor_name].between(
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
        fig_diff.update_xaxes(
            title_text=f"{predictor_name} (binned)",
        )
        fig_diff.update_yaxes(title_text="count", secondary_y=False)

        fig_diff.update_yaxes(title_text=f"{response}", secondary_y=True)

        fig_diff.update_yaxes(range=(0.3, 0.7), constrain="domain", secondary_y=True)

        fig_diff_file_save = (
            f"{path}/results/{response_html}_{predictor_name_html}_diff.html"
        )

        fig_diff_file_link = f"{response_html}_{predictor_name_html}_diff.html"

        fig_diff.write_html(file=fig_diff_file_save, include_plotlyjs="cdn")

        DMR_file_link = "<a target='blank' href=" + fig_diff_file_link + ">PLOT</a>"

        # Add values to results table
        results = results.append(
            {
                "Response": response,
                "Predictor": predictor_name,
                "t Score": t_score,
                "p Value": p_value,
                "Reg Plot": reg_file_link,
                "Diff Mean of Response (Unweighted)": msd_uw,
                "Diff Mean of Response (Weighted)": msd_w,
                "DMR Plot": DMR_file_link,
            },
            ignore_index=True,
        )

    # get random forest importance values for each predictor

    Models_cols = [
        "Model",
        "Score",
        "Confusion Matrix",
    ]
    Models = pandas.DataFrame(columns=Models_cols)

    x = ["Actual Loss", "Actual Win"]
    y = ["Predicted Loss", "Predicted Win"]

    z_text = [["True Loss", "False Win"], ["False Loss", "True Win"]]

    X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(
        predictor_columns, response_feature, test_size=0.2, random_state=56
    )

    sc = sklearn.preprocessing.StandardScaler()
    X_train = sc.fit_transform(X_train)
    X_test = sc.fit_transform(X_test)
    X_train = sklearn.preprocessing.normalize(X_train)
    X_test = sklearn.preprocessing.normalize(X_test)

    regr = sklearn.linear_model.LogisticRegression()

    # Train the model using the training sets
    regr.fit(X_train, y_train)

    # Make predictions using the testing set
    regr_y_pred = regr.predict(X_test)

    regr_score = regr.score(X_test, y_test)

    regr_z = sklearn.metrics.confusion_matrix(y_test, regr_y_pred.round())

    regr_fig = plotly.figure_factory.create_annotated_heatmap(
        regr_z, x=x, y=y, annotation_text=z_text, colorscale="Viridis", showscale=True
    )

    regr_fig.update_layout(
        title_text=f"Logistic Confusion Matrix<br>Accuracy = {regr_score}", title_x=0.5
    )

    regr_fig_file_save = f"{path}/results/LogisticRegressionConfusionMatrix.html"

    regr_fig.write_html(file=regr_fig_file_save, include_plotlyjs="cdn")

    regr_ConfusionMatrix_file_link = (
        "<a target='blank' href=LogisticRegressionConfusionMatrix.html>Matrix</a>"
    )

    Models = Models.append(
        {
            "Model": "Logistic Regression (All Predictors)",
            "Score": regr_score,
            "Confusion Matrix": regr_ConfusionMatrix_file_link,
        },
        ignore_index=True,
    )

    model = RandomForestClassifier(n_estimators=1000)

    model.fit(X_train, y_train)

    rf_y_pred = model.predict(X_test)

    rf_score = model.score(X_test, y_test)

    rf_z = sklearn.metrics.confusion_matrix(y_test, rf_y_pred)

    rf_fig = plotly.figure_factory.create_annotated_heatmap(
        rf_z, x=x, y=y, annotation_text=z_text, colorscale="Viridis", showscale=True
    )

    rf_fig.update_layout(
        title_text=f"Random Forest (All Features) Confusion Matrix<br>Accuracy = {rf_score}",
        title_x=0.5,
    )

    rf_fig_file_save = f"{path}/results/RandomForestConfusionMatrix.html"

    rf_fig.write_html(file=rf_fig_file_save, include_plotlyjs="cdn")

    ConfusionMatrix_file_link = (
        "<a target='blank' href=RandomForestConfusionMatrix.html>Matrix</a>"
    )

    Models = Models.append(
        {
            "Model": "Random Forest (All Features)",
            "Score": rf_score,
            "Confusion Matrix": ConfusionMatrix_file_link,
        },
        ignore_index=True,
    )

    importance = model.feature_importances_
    importancedataframe = pandas.DataFrame(columns=["Random Forest Importance"])
    importancedataframe["Random Forest Importance"] = importance

    # append importance values to table of results
    results = pandas.concat([results, importancedataframe], axis=1)

    results = results.sort_values(["Diff Mean of Response (Weighted)"], ascending=False)

    results.to_html(f"{path}/results/Results Table.html", escape=False)

    Models.to_html(f"{path}/results/Models Table.html", escape=False)


if __name__ == "__main__":
    sys.exit(main())
