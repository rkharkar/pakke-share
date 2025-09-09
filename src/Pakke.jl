module Pakke

using DataFrames
using Plots
using Plots.PlotMeasures
using DataFramesMeta
using StatsBase

include("importers.jl");
include("analysis.jl");
include("visualization.jl");
include("utils.jl");

prj_data_path = "../data/Elephants PRT.boris";
milestones_data_path = "../data/milestones_orig.csv";
output_dir = "../output";

println("Reading and processing BORIS project file...");
project_data = Importers.read_boris_project(prj_data_path, milestones_data_path);

println("Project summary:");
println("  Number of subjects: ", nrow(project_data["subjects"]));
println("  Number of observations: ", nrow(project_data["observations"]));
println("  Number of behaviors: ", nrow(project_data["behaviors"]));
println("  Number of events: ", nrow(project_data["events"]));

println("\nAnalysis");
println("\nComputing time budgets for individual behaviors");
time_budgets = Analysis.calc_time_budgets(
    project_data["events"],
    project_data["behaviors"],
    project_data["observations"]
);

println("Computing time budgets for individual categories");
time_budgets_by_cat = Analysis.calc_time_budgets_by_cat(
    project_data["events"],
    project_data["behaviors"],
    project_data["observations"]
);

println("Writing time budgets by category file");
@chain time_budgets_by_cat begin
    innerjoin(_, project_data["observations"], on = :obs_id)
    innerjoin(_, project_data["subjects"], on = :subject_id => :id)
    innerjoin(_, project_data["categories"], on = :category_id => :id)
    @groupby([:name, :category])
    combine(_) do gdf
        test_results = Utils.spearman_test(gdf.prop_time_spent, gdf.session)
        return (
            name = first(gdf.name),
            category = first(gdf.category),
            correlation = test_results.correlation,
            p_value = test_results.p_value,
            t_stat = test_results.t_statistic,
        )
    end
    Utils.safe_save_csv(_, "../output/data/state_behaviors_correlations_by_subject.csv")
end

println("Computing point behavior and category counts per session");
point_behavior_counts = Analysis.calc_point_behaviors_count_by_session(
    project_data["events"],
    project_data["behaviors"],
    project_data["observations"]
);
point_behavior_category_counts = Analysis.calc_point_behaviors_category_count_by_session(
    project_data["events"],
    project_data["behaviors"],
    project_data["observations"]
);

println("Writing point behaviors by category file");
@chain point_behavior_category_counts begin
    innerjoin(_, project_data["observations"], on = :obs_id)
    innerjoin(_, project_data["subjects"], on = :subject_id => :id)
    innerjoin(_, project_data["categories"], on = :category_id => :id)
    @groupby([:name, :category])
    combine(_) do gdf
        test_results = Utils.spearman_test(gdf.frequency, gdf.session)
        return (
            name = first(gdf.name),
            category = first(gdf.category),
            correlation = test_results.correlation,
            p_value = test_results.p_value,
            t_stat = test_results.t_statistic,
        )
    end
    Utils.safe_save_csv(_, "../output/data/point_behaviors_correlations_by_subject.csv")
end;

println("\nComputing mean time budgets and counts");
println("Mean time budgets per behavior");
mean_time_budgets = Analysis.average_time_budgets(
    project_data["events"],
    project_data["behaviors"],
    project_data["observations"]
);

println("Mean time budgets per category");
mean_time_budgets_categories = Analysis.average_time_budgets(
    project_data["events"],
    project_data["behaviors"],
    project_data["observations"],
    categories = true
);

println("Writing mean time budgets file")
@chain mean_time_budgets_categories begin
    innerjoin(_, project_data["categories"], on = :category_id => :id)
    @groupby(:category)
    combine(_) do gdf
        test_results = Utils.spearman_test(gdf.mean_time_spent, gdf.session)
        return (
            category = first(gdf.category),
            correlation = test_results.correlation,
            p_value = test_results.p_value,
            t_stat = test_results.t_statistic,
        )
    end
    Utils.safe_save_csv(_, "../output/data/state_behaviors_correlations.csv")
end;

println("Mean counts for point behaviors");
mean_point_behavior_counts = Analysis.average_point_behavior_counts(
    project_data["events"],
    project_data["behaviors"],
    project_data["observations"]
);

println("Mean counts for point behavior categories");
mean_point_behavior_category_counts = Analysis.average_point_behavior_counts(
    project_data["events"],
    project_data["behaviors"],
    project_data["observations"],
    categories = true
);

println("Writing out mean point behaviors by category file");
@chain mean_point_behavior_category_counts begin
    innerjoin(_, project_data["categories"], on = :category_id => :id)
    @groupby(:category)
    combine(_) do gdf
        test_results = Utils.spearman_test(gdf.mean_frequency, gdf.session)
        return (
            category = first(gdf.category),
            correlation = test_results.correlation,
            p_value = test_results.p_value,
            t_stat = test_results.t_statistic,
        )
    end
    Utils.safe_save_csv(_, "../output/data/point_behaviors_correlations.csv")
end;

println("Dealing with stress in sessions");
all_sessions_stress = Analysis.were_sessions_stressy(
    time_budgets_by_cat,
    point_behavior_category_counts,
    project_data["observations"],
    project_data["categories"],
    project_data["subjects"]
);
stressiness_of_milestones = @rsubset(all_sessions_stress, !ismissing(:milestone_ids));

Utils.safe_save_csv(stressiness_of_milestones, "../output/data/milestones_stress.csv");

println("Common stressors/non-stressors");
@chain stressiness_of_milestones begin
    Analysis.common_stressors(_)
    @by(:stressed, :num_milestones = length(:milestone_ids))
    println(_)
end;

stress_model = Analysis.fit_model(all_sessions_stress);
    
println("\nGenerating visualizations");
println("\nGenerating time series plots");
timelines = Visualization.plot_events_all_sessions(
    project_data["events"],
    project_data["observations"],
    project_data["subjects"],
    project_data["behaviors"]
);

println("Generating graphs for time budgets for behaviors");
budget_plots = Visualization.plot_series(
    time_budgets,
    project_data["observations"],
    project_data["behaviors"],
    project_data["subjects"],
    "prop_time_spent",
    "_behaviors",
    ("Behaviors", "Session number")
);

println("Generating graphs for time budgets for categories");
budget_plots_cats = Visualization.plot_category_series(
    time_budgets_by_cat,
    project_data["observations"],
    project_data["categories"],
    project_data["subjects"],
    "prop_time_spent",
    "_categories",
    ("Proportion of time spent", "Session number")
);

println("Generating graphs for point behavior counts and frequencies");
counts_plots = Visualization.plot_series(
    point_behavior_counts,
    project_data["observations"],
    project_data["behaviors"],
    project_data["subjects"],
    "count",
    "_point_behaviors_counts",
    ("Raw count", "Session number")
);
frequency_plots = Visualization.plot_series(
    point_behavior_counts,
    project_data["observations"],
    project_data["behaviors"],
    project_data["subjects"],
    "frequency",
    "_point_behaviors_frequencies",
    ("Behavior frequency (per minute)", "Session number")
);

println("Generating graphs for point behavior category counts and frequencies");
category_counts_plots = Visualization.plot_category_series(
    point_behavior_category_counts,
    project_data["observations"],
    project_data["categories"],
     project_data["subjects"],
    "count",
    "_point_behaviors_category_counts",
    ("Raw count", "Session number")
);
category_frequency_plots = Visualization.plot_category_series(
    point_behavior_category_counts,
    project_data["observations"],
    project_data["categories"],
     project_data["subjects"],
    "frequency",
    "_point_behaviors_category_frequencies",
    ("Frequency (per minute)", "Session number")
);

println("Generating graphs for means");
behavior_time_budgets_mean_plot = Visualization.plot_mean_budgets_facets(
    mean_time_budgets,
    project_data["behaviors"]
);
behavior_time_budgets_categories_mean_plot = Visualization.plot_mean_budgets_categories(
    mean_time_budgets_categories,
    project_data["categories"]
);
behavior_counts_mean_plots = Visualization.plot_mean_behavior_counts_facets(
    mean_point_behavior_counts,
    project_data["behaviors"]
);
behavior_category_counts_mean_plots = Visualization.plot_mean_behavior_categories_counts(
    mean_point_behavior_category_counts,
    project_data["categories"]
);

println("\nSaving figures");
println("Saving event timelines");
Utils.save_figs_in_dict(timelines, output_dir);

println("Saving time budget plots");
Utils.save_figs_in_dict(budget_plots, output_dir);
Utils.save_figs_in_dict(budget_plots_cats, output_dir);

println("Saving point behavior summaries");
Utils.save_figs_in_dict(counts_plots, output_dir);
Utils.save_figs_in_dict(frequency_plots, output_dir);
Utils.save_figs_in_dict(category_counts_plots, output_dir);
Utils.save_figs_in_dict(category_frequency_plots, output_dir);

println("Saving means");
Utils.safe_save(behavior_time_budgets_mean_plot, string(output_dir, "/", "mean_time_budgets"));
Utils.safe_save(
    behavior_time_budgets_categories_mean_plot,
    string(output_dir, "/", "mean_time_budgets_categories")
);
Utils.safe_save(
    behavior_counts_mean_plots[1],
    string(output_dir, "/", "mean_point_behavior_counts")
);
Utils.safe_save(
    behavior_counts_mean_plots[2],
    string(output_dir, "/", "mean_point_behavior_frequencies")
);
Utils.safe_save(
    behavior_category_counts_mean_plots,
    string(output_dir, "/", "mean_point_behavior_category_frequencies")
);

println("Custom plots for paper");
println("Figure 1: Means facet plot");
l = @layout [a{0.395w} b];
plot!(
    behavior_time_budgets_categories_mean_plot,
    legend = :none
)
plot!(
    behavior_category_counts_mean_plots,
    legend = :outertopright
)
means_plot = plot(
    behavior_time_budgets_categories_mean_plot,
    behavior_category_counts_mean_plots,
    layout = l,
    size = (2400, 1200),
    top_margin = 0px,
    left_margin = 80px,
    right_margin = 80px,
    bottom_margin = 180px,
    guidefont = (32, :black),
    tickfont = (24, :black),
    legendfont = (24, :black)
);
annotate!(
    means_plot[1],
    125,
    -0.08,
    text("Session Number", 32, :center, color = :black)
)
annotate!(
    means_plot[1],
    58,
    -0.13,
    text("(a)", 24, :center, color = :black)
)
annotate!(
    means_plot[2],
    58,
    -0.99,
    text("(b)", 24, :center, color = :black)
)
Utils.safe_save(means_plot, string(output_dir, "/", "figure_1_means"));

println("Figure 2: Individual subjects state behavior categories");
khaisingh_cats = plot(
    budget_plots_cats["Khaisingh_categories"],
    xlabel = "",
    ylabel = "",
    title = "",
    guidefont = (32, :black),
    legendfont = (24, :black),
    tickfont = (24, :black),
    legend = :none
);
bahadur_cats = plot(
    budget_plots_cats["Bahadur_categories"],
    xlabel = "",
    ylabel = "",
    title = "",
    guidefont = (32, :black),
    legendfont = (24, :black),
    tickfont = (24, :black),
    legend = :none
);
vijaya_cats = plot(
    budget_plots_cats["Vijaya_categories"],
    xlabel = "Session Number",
    ylabel = "",
    title = "",
    guidefont = (32, :black),
    legendfont = (24, :black),
    tickfont = (24, :black),
    legend = :outertopright
);
l = @layout [a b; _ c{0.75w} _];
cats_plot = plot(
    khaisingh_cats,
    bahadur_cats,
    vijaya_cats,
    layout = l,
    size = (2400, 1200),
    top_margin = 0px,
    right_margin = 80px,
    bottom_margin = 120px,
    left_margin = 100px
);
annotate!(
    cats_plot[1],
    -20,
    -0.4,
    text("Proportion of Time", 32, :center, color = :black, rotation = 90)
)
annotate!(
    cats_plot[1],
    58,
    -0.25,
    text("(a)", 24, :center, color = :black)
)
annotate!(
    cats_plot[2],
    58,
    -0.16,
    text("(b)", 24, :center, color = :black)
)
annotate!(
    cats_plot[3],
    58,
    -0.4,
    text("(c)", 24, :center, color = :black)
)
Utils.safe_save(
    cats_plot,
    string(output_dir, "/", "figure_2_state_behaviors")
);

println("Figure 3: Individual subjects point behavior categories");
khaisingh_freq = plot(
    category_frequency_plots["Khaisingh_point_behaviors_category_frequencies"],
    xlabel = "",
    ylabel = "",
    title = "",
    guidefont = (32, :black),
    legendfont = (24, :black),
    tickfont = (24, :black),
    legend = :none
);
bahadur_freq = plot(
    category_frequency_plots["Bahadur_point_behaviors_category_frequencies"],
    xlabel = "",
    ylabel = "",
    title = "",
    guidefont = (32, :black),
    legendfont = (24, :black),
    tickfont = (24, :black),
    legend = :none
);
vijaya_freq = plot(
    category_frequency_plots["Vijaya_point_behaviors_category_frequencies"],
    xlabel = "Session Number",
    ylabel = "",
    title = "",
    legend = :outertopright,
    titlefont = (48, :black),
    guidefont = (32, :black),
    legendfont = (24, :black),
    tickfont = (24, :black)
);
vijaya_freq.series_list[1].plotattributes[:linecolor] = palette(:auto)[2]
vijaya_freq.series_list[3].plotattributes[:linecolor] = palette(:auto)[2]
vijaya_freq.series_list[2].plotattributes[:linecolor] = palette(:auto)[1]
vijaya_freq.series_list[4].plotattributes[:linecolor] = palette(:auto)[1]
freq_plot = plot(
    khaisingh_freq,
    bahadur_freq,
    vijaya_freq,
    layout = l,
    size = (2400, 1200),
    top_margin = 0px,
    right_margin = 80px,
    bottom_margin = 120px,
    left_margin = 100px
);
annotate!(
    freq_plot[1],
    -15,
    -2,
    text("Frequency (/min)", 32, :center, color = :black, rotation = 90)
)
annotate!(
    freq_plot[1],
    58,
    -1.25,
    text("(a)", 24, :center, color = :black)
)
annotate!(
    freq_plot[2],
    58,
    -2.1,
    text("(b)", 24, :center, color = :black)
)
annotate!(
    freq_plot[3],
    58,
    -4,
    text("(c)", 24, :center, color = :black)
)
Utils.safe_save(
    freq_plot,
    string(output_dir, "/", "figure_3_point_behaviors")
);

l = @layout [a b; _ c{0.5w} _]
println("State behaviours time budgets heatmap");
khaisingh_heatmap = plot(
    budget_plots["Khaisingh_behaviors"],
    xlabel = "(a)",
    ylabel = "",
    title = "",
    guidefont = (24, :black),
    legendfont = (24, :black),
    tickfont = (24, :black)
);
bahadur_heatmap = plot(
    budget_plots["Bahadur_behaviors"],
    xlabel = "(b)",
    ylabel = "",
    title = "",
    guidefont = (24, :black),
    legendfont = (24, :black),
    tickfont = (24, :black)
);
vijaya_heatmap = plot(
    budget_plots["Vijaya_behaviors"],
    xlabel = "Session Number",
    ylabel = "",
    title = "",
    guidefont = (24, :black),
    legendfont = (24, :black),
    tickfont = (24, :black)
);
budgets_state_heatmap_plot = plot(
    khaisingh_heatmap,
    bahadur_heatmap,
    vijaya_heatmap,
    layout = l,
    size = (2400, 1200),
    top_margin = 50px,
    right_margin = 0px,
    bottom_margin = 120px,
    left_margin = 100px
);
annotate!(
    budgets_state_heatmap_plot[3],
    58,
    -4.5,
    text("(c)", 24, :center, color = :black)
)
Utils.safe_save(
    budgets_state_heatmap_plot,
    string(output_dir, "/", "figure_A1_state_heatmaps")
);

println("Point behaviors frequencies heatmap");
khaisingh_heatmap = plot(
    frequency_plots["Khaisingh_point_behaviors_frequencies"],
    xlabel = "(a)",
    ylabel = "",
    title = "",
    guidefont = (24, :black),
    legendfont = (24, :black),
    tickfont = (24, :black)
);
bahadur_heatmap = plot(
    frequency_plots["Bahadur_point_behaviors_frequencies"],
    xlabel = "(b)",
    ylabel = "",
    title = "",
    guidefont = (24, :black),
    legendfont = (24, :black),
    tickfont = (24, :black)
);
vijaya_heatmap = plot(
    frequency_plots["Vijaya_point_behaviors_frequencies"],
    xlabel = "Session Number",
    ylabel = "",
    title = "",
    guidefont = (24, :black),
    legendfont = (24, :black),
    tickfont = (24, :black)
);
budgets_point_heatmap_plot = plot(
    khaisingh_heatmap,
    bahadur_heatmap,
    vijaya_heatmap,
    layout = l,
    size = (2400, 1200),
    top_margin = 50px,
    right_margin = 0px,
    bottom_margin = 120px,
    left_margin = 100px
);
annotate!(
    budgets_point_heatmap_plot[3],
    58,
    -2,
    text("(c)", 24, :center, color = :black)
)
Utils.safe_save(
    budgets_point_heatmap_plot,
    string(output_dir, "/", "figure_A2_point_heatmaps")
);
end
