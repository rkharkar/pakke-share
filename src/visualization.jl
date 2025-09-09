module Visualization

export plot_events_all_sessions, plot_response_to_treats_for_all, plot_series, plot_category_series;

using Plots
using DataFrames
using StatsPlots
using DataFramesMeta
using Dates
using StatsBase

include("utils.jl")
using .Utils


function plot_events(events_df::AbstractDataFrame, behaviors_df::DataFrame)
    obs_id = first(events_df.obs_id);
    if isempty(events_df)
        return plot(title="No events found for observation $obs_id",
                   xlabel="Time (s)", ylabel="Behavior");
    end
    
    behavior_map = Dict(row.id => row.code for row in eachrow(behaviors_df));
    
    p = plot(title="Event Timeline for Observation $obs_id",
             xlabel="Time (s)", 
             ylabel="Behavior",
             legend=false);
    
    unique_behaviors = sort(unique(events_df.behavior_id));
    
    behavior_y = Dict(id => i for (i, id) in enumerate(unique_behaviors));
    
    yticks_pos = [behavior_y[id] for id in unique_behaviors];
    yticks_labels = [behavior_map[id] for id in unique_behaviors];
    
    for row in eachrow(events_df)
        y_pos = behavior_y[row.behavior_id];
        
        if ismissing(row.stop)
            scatter!([row.start], [y_pos], markersize = 6, color = :blue, shape = :circle);
        else
            plot!([row.start, row.stop], [y_pos, y_pos], linewidth = 6, color = :green);
        end
    end
    
    plot!(yticks=(yticks_pos, yticks_labels));
    
    return p;
end

function plot_events_all_sessions(
    events_df::DataFrame,
    observations_df::DataFrame,
    subjects_df::DataFrame,
    behaviors_df::DataFrame
    )
    observations_with_plots = @chain events_df begin
        innerjoin(_, observations_df, on = :obs_id)
        innerjoin(_, subjects_df, on = :subject_id => :id)
        @groupby(_, :obs_id)
        combine(_) do gdf
            return (
                date_string = Dates.format(first(gdf.date), "yyyy-mm-dd"),
                sub_session = string(first(gdf.name), first(gdf.session)),
                events_plot = () -> plot_events(gdf, behaviors_df)
            )
        end
    end;

    date_groups = groupby(observations_with_plots, :date_string);

    return Dict(
        first(group.date_string) =>
            Dict(row.sub_session => row.events_plot for row in eachrow(group))
        for group in date_groups
    );
end

function add_minimal_event_markers!(p, time_budget, column)
    observations_with_milestones = @chain time_budget begin
        @select(:session, :milestone_ids)
        @subset(.!ismissing.(:milestone_ids))
        unique(_)
    end

    y_limits = ylims(p);
    high_y = y_limits[2] * 0.95;
    low_y = y_limits[2] * 0.85;
    for (i, row) in enumerate(eachrow(observations_with_milestones))
        vline!(
            [row.session],
            linestyle = :dot,
            linewidth = 2,
            color = :red,
            alpha = 0.8,
            label = ""
        );
        # annotation_text = join(row.milestone_ids, ",");
        # annotation_y = iseven(i) ? high_y : low_y;
        
        # annotate!(
        #     row.session,
        #     annotation_y,
        #     text(annotation_text, 16, :center, color=:black, rotation = 90)
        # );
    end    
end

function add_minimal_event_markers_heatmap!(p, time_budget, y_ticks)
    sessions = sort(unique(time_budget[:, "session"]));
    session_to_index = Dict(session => i for (i, session) in enumerate(sessions));
    
    observations_with_milestones = @chain time_budget begin
        @select(:session, :milestone_ids)
        @subset(.!ismissing.(:milestone_ids))
        unique(_)
    end
    
    for (i, row) in enumerate(eachrow(observations_with_milestones))
        session_index = session_to_index[row.session]
        
        scatter!(
            [session_index],
            [length(y_ticks) + 0.7],
            marker = :diamond,
            markersize = 8,
            color = :red,
            label = ""
        );
        
        scatter!(
            [session_index],
            [0.3],
            marker = :diamond,
            markersize = 8,
            color = :red,
            label = ""
        );
    end    
end

function plot_for_subject(
    time_budget::DataFrame,
    subject::String,
    groupby::Symbol,
    column::String,
    create_heatmap::Bool,
    axes_labels::Tuple{String, String},
    max_ticks::Int = 10
    )
    time_budget_for_subject = @rsubset(time_budget, :name == subject);
    
    if create_heatmap == false
        p = plot(
            time_budget_for_subject[:, "session"],
            time_budget_for_subject[:, column],
            group = time_budget_for_subject[:, groupby],
            title = subject,
            ylabel = axes_labels[1],
            xlabel = axes_labels[2],
            grid = false,
            legend = :right,
            linewidth = 3
        );

        group_means = @by(time_budget_for_subject, groupby, :mean_value = mean($column));

        unique_groups = sort(unique(time_budget_for_subject[:, groupby]));

        for (i, group_name) in enumerate(unique_groups)
            mean_val = @rsubset(group_means, string(cols(groupby)) == group_name).mean_value[1];
            
            hline!(
                p,
                [mean_val],
                line = (:dot, 2),
                color = i,
                label = "Mean $(group_name)",
                alpha = 0.8
            )
        end
        
        add_minimal_event_markers!(p, time_budget_for_subject, column);
        return p
        
    else
        stat_matrix = Utils.create_stat_matrix(time_budget_for_subject, column);
        y_ticks = unique(time_budget_for_subject[:, String(groupby)]);
        
        p = heatmap(
            stat_matrix,
            title = "$(column) for $(subject)",
            ylabel = axes_labels[1],
            xlabel = axes_labels[2],
            yticks = (1:length(y_ticks), y_ticks),
            color = :viridis
        );
        
        add_minimal_event_markers_heatmap!(p, time_budget_for_subject, y_ticks);
        
        return p;
    end
end

function plot_series(
    time_budgets::DataFrame,
    observations_df::DataFrame,
    behaviors_df::DataFrame,
    subjects_df::DataFrame,
    column::String,
    suffix::String,
    axes_labels::Tuple{String, String}
    )
    time_budgets_joined = @chain time_budgets begin
        innerjoin(_, observations_df, on = :obs_id)
        innerjoin(_, behaviors_df, on = :behavior_id => :id)
        @select(Not(:description))
        innerjoin(_, subjects_df, on = :subject_id => :id)
        sort(_, [:date, :session])
        @select(:code, :name, :session, $(column), :milestone_ids)
    end;

    subjects = unique(time_budgets_joined[:, :name]);
    return Dict(
        string(subject, suffix) => plot_for_subject(
            time_budgets_joined,
            subject,
            :code,
            column,
            true,
            axes_labels,
        ) for subject in subjects);
end

function plot_category_series(
    time_budgets::DataFrame,
    observations_df::DataFrame,
    categories_df::DataFrame,
    subjects_df::DataFrame,
    column::String,
    suffix::String,
    axes_labels::Tuple{String, String},
    )
    time_budgets_joined = @chain time_budgets begin
        innerjoin(_, observations_df, on = :obs_id)
        innerjoin(_, categories_df, on = :category_id => :id)
        innerjoin(_, subjects_df, on = :subject_id => :id)
        sort(_, [:date, :session])
        @select(:category, :name, :session, $(column), :milestone_ids)
        @rsubset(:category == "Avoidance" || :category == "Displacement")
    end;
    
    subjects = unique(time_budgets_joined[:, :name]);
    return Dict(string(subject, suffix) => plot_for_subject(
        time_budgets_joined,
        subject,
        :category,
        column,
        false,
        axes_labels
    ) for subject in subjects);
end

function plot_mean_budgets_facets(
    results_df::DataFrame,
    behaviors_df::DataFrame
    )
    joined_df = innerjoin(results_df, behaviors_df, on = :behavior_id => :id);
    behaviors = unique(joined_df.code);
    n_behaviors = length(behaviors);
    
    plots = map(behaviors) do behavior
        behavior_data = subset(joined_df, :code => x -> x .== behavior);
        
        plot(
            behavior_data.session, behavior_data.mean_time_spent,
            fillalpha = 0.3,
            linewidth = 2,
            marker = :circle,
            title = "$(behavior)",
            xlabel = "Session",
            ylabel = "Proportion Time",
            ylims = (0, 1),
            legend = false
        );
    end
    
    cols = min(3, n_behaviors);
    rows = ceil(Int, n_behaviors / cols);
    combined_plot = plot(plots..., layout = (rows, cols));
    
    return combined_plot;
end

function plot_mean_budgets_categories(
    results_df::DataFrame,
    categories_df::DataFrame
    )
    joined_df = @chain results_df begin
        innerjoin(_, categories_df, on = :category_id => :id)
        @rsubset(:category in ["Avoidance", "Displacement"])
    end;

    return plot(
        joined_df[:, :session],
        joined_df[:, :mean_time_spent],
        group = joined_df[:, :category],
        ylabel = "Proportion of Time",
        legend = :topright,
        linewidth = 3
    )
end

function plot_mean_behavior_counts_facets(
    results_df::DataFrame,
    behaviors_df::DataFrame
    )
    joined_df = innerjoin(results_df, behaviors_df, on = :behavior_id => :id);
    behaviors = unique(joined_df.code);
    n_behaviors = length(behaviors);
    
    plot_pairs = map(behaviors) do behavior
        behavior_data = subset(joined_df, :code => x -> x .== behavior);
        
        count_plot = plot(
            behavior_data.session, behavior_data.mean_count,
            fillalpha = 0.3,
            linewidth = 2,
            marker = :circle,
            title = "$(behavior)",
            xlabel = "Session",
            ylabel = "Count",
            legend = false
        );

        frequency_plot = plot(
            behavior_data.session, behavior_data.mean_frequency,
            fillalpha = 0.3,
            linewidth = 2,
            marker = :circle,
            title = "$(behavior)",
            xlabel = "Session",
            ylabel = "Frequency",
            legend = false
        );

        (count_plot, frequency_plot);
    end

    count_plots = first.(plot_pairs);
    frequency_plots = last.(plot_pairs);
    
    cols = min(3, n_behaviors);
    rows = ceil(Int, n_behaviors / cols);
    combined_counts_plot = plot(
        count_plots...,
        layout = (rows, cols)
    );
    combined_frequencies_plot = plot(
        frequency_plots...,
        layout = (rows, cols)
    );
    
    return (combined_counts_plot, combined_frequencies_plot);
end

function plot_mean_behavior_categories_counts(
    results_df::DataFrame,
    categories_df::DataFrame
    )
    joined_df = @chain results_df begin
        innerjoin(_, categories_df, on = :category_id => :id)
        @rsubset(:category in ["Avoidance", "Displacement"])
    end;

    return plot(
        joined_df[:, :session],
        joined_df[:, :mean_frequency],
        group = joined_df[:, :category],
        ylabel = "Frequency (/min)",
        legend = :topright,
        linewidth = 3
    );
end

end
