module Analysis

export calc_time_budgets,
    calc_time_budgets_by_cat,
    calc_point_behaviors_count_by_session,
    calc_point_behaviors_category_count_by_session,
    average_time_budgets,
    were_sessions_stressy,
    common_stressors,
    fit_model

using DataFrames
using Statistics
using DataFramesMeta
using Dates
using StatsBase
using GLM

include("utils.jl")

function calc_time_budgets(events_df::DataFrame, behaviors_df::DataFrame, observations_df::DataFrame)
    return @chain events_df begin
        innerjoin(_, behaviors_df, on = :behavior_id => :id)
        @rsubset(:type == "State event")
        innerjoin(_, observations_df, on = :obs_id => :obs_id)
        @by(
            [:obs_id, :behavior_id],
            :prop_time_spent = sum(:stop - :start) / first(:duration)
        );
    end;
end

function merge_intervals(df::AbstractDataFrame)
    sorted_df = sort(df, :start);

    if nrow(sorted_df) == 0
        return DataFrame(stop = Float64[], start = Float64[]);
    end

    merged = DataFrame(stop = Float64[], start = Float64[]);
    current_start = sorted_df[1, :start];
    current_stop = sorted_df[1, :stop];

    for i in 2:nrow(sorted_df)
        if sorted_df[i, :start] <= current_stop
            current_stop = max(current_stop, sorted_df[i, :stop]);
        else
            push!(merged, (stop = current_stop, start = current_start));
            current_stop = sorted_df[i, :stop];
            current_start = sorted_df[i, :start];
        end
    end

    push!(merged, (start = current_start, stop = current_stop));
    return merged;
end

function calc_total_merged_time(gdf::AbstractDataFrame)
    merged = merge_intervals(gdf);

    return sum(merged.stop .- merged.start);
end

function calc_time_budgets_by_cat(
    events_df::DataFrame,
    behaviors_df::DataFrame,
    observations_df::DataFrame
    )
    return @chain events_df begin
        innerjoin(_, behaviors_df, on = :behavior_id => :id)
        innerjoin(_, observations_df, on = :obs_id => :obs_id)
        @rsubset(
            :type == "State event",
            :code !== "Leg held"
        )
        groupby(_, [:obs_id, :category_id])
        combine(_) do gdf
            total_time = calc_total_merged_time(gdf)
            duration = first(gdf.duration)
            prop_time = total_time / duration
            return (prop_time_spent = prop_time,)
        end
    end
end

function calc_point_behaviors_count_by_session(
    events_df::DataFrame,
    behaviors_df::DataFrame,
    observations_df::DataFrame
    )
    return @chain events_df begin
        innerjoin(_, behaviors_df, on = :behavior_id => :id)
        @rsubset(:type == "Point event")
        innerjoin(_, observations_df, on = :obs_id)
        @by(
            [:obs_id, :behavior_id],
            :count = length(:start),
            :frequency = length(:start) / (first(:duration) / 60)
        )
    end;
end

function calc_point_behaviors_category_count_by_session(
    events_df::DataFrame,
    behaviors_df::DataFrame,
    observations_df::DataFrame
    )
    return @chain events_df begin
        innerjoin(_, behaviors_df, on = :behavior_id => :id)
        @rsubset(
            :type == "Point event"
            # :code !== "Leg moved"
        )
        innerjoin(_, observations_df, on = :obs_id)
        @by(
            [:obs_id, :category_id],
            :count = length(:start),
            :frequency = length(:start) / (first(:duration) / 60)
        )
    end;
end

function average_time_budgets(
    events_df::DataFrame,
    behaviors_df::DataFrame,
    observations_df::DataFrame;
    categories::Bool = false
    )
    if categories
        time_budgets = calc_time_budgets_by_cat(events_df, behaviors_df, observations_df);
        grouping_cols = [:session, :category_id];
    else
        time_budgets = calc_time_budgets(events_df, behaviors_df, observations_df);
        grouping_cols = [:session, :behavior_id];
    end
    
    return @chain time_budgets begin
        innerjoin(_, observations_df, on = :obs_id)
        @by(
            grouping_cols,
            :mean_time_spent = mean(:prop_time_spent),
            :n_observations = length(:prop_time_spent)
        )
    end;
end

function average_point_behavior_counts(
    events_df::DataFrame,
    behaviors_df::DataFrame,
    observations_df::DataFrame;
    categories::Bool = false
    )
    if categories
        counts = calc_point_behaviors_category_count_by_session(
            events_df,
            behaviors_df,
            observations_df
        );
        grouping_cols = [:session, :category_id];
    else
        counts = calc_point_behaviors_count_by_session(
            events_df,
            behaviors_df,
            observations_df
        );
        grouping_cols = [:session, :behavior_id];
    end

    return @chain counts begin
        innerjoin(_, observations_df, on = :obs_id)
        @by(
            grouping_cols,
            :n_observations = length(:count),
            :mean_count = mean(:count),
            :mean_frequency = mean(:frequency)
        )
        @orderby(:session)
    end
end

function were_sessions_stressy(
    state_cats_budget::DataFrame,
    point_cats_counts::DataFrame,
    observations_df::DataFrame,
    categories_df::DataFrame,
    subjects_df::DataFrame
    )
    budget_means = @chain state_cats_budget begin
        innerjoin(_, observations_df, on = :obs_id)
        @by([:subject_id, :category_id], :mean_prop = mean(:prop_time_spent))
    end;

    freq_means = @chain point_cats_counts begin
        innerjoin(_, observations_df, on = :obs_id)
        @by([:subject_id, :category_id], :mean_freq = mean(:frequency))
    end;

    state_was_stressy = @chain observations_df begin
        innerjoin(_, state_cats_budget, on = :obs_id)
        innerjoin(_, budget_means, on = [:subject_id, :category_id])
        innerjoin(_, observations_df, on = :obs_id, makeunique = true)
        @select(
            :subject_id,
            :milestone_ids,
            :category_id,
            :date,
            :session,
            :was_stressy = :prop_time_spent .> :mean_prop
        )
    end;

    point_was_stressy = @chain observations_df begin
        innerjoin(_, point_cats_counts, on = :obs_id)
        innerjoin(_, freq_means, on = [:subject_id, :category_id])
        innerjoin(_, observations_df, on = :obs_id, makeunique = true)
        @select(
            :subject_id,
            :milestone_ids,
            :category_id,
            :date,
            :session,
            :was_stressy = :frequency .> :mean_freq
        )
    end;

    return @chain state_was_stressy begin
        vcat(_, point_was_stressy, cols = :orderequal)
        innerjoin(_, categories_df, on = :category_id => :id)
        innerjoin(_, subjects_df, on = :subject_id => :id)
        @rsubset(:category in ["Avoidance", "Displacement"])
        @by([:name, :milestone_ids, :session], :was_stressy = any(:was_stressy))
    end;
end

function common_stressors(stress_df::AbstractDataFrame)
    flattened_stress_df = flatten(stress_df, :milestone_ids);
    all_stressed = @chain flattened_stress_df begin
        @by(:milestone_ids, :all_stressed = all(:was_stressy))
        @rsubset(:all_stressed == true)
        @select(:milestone_ids, :stressed = true)
    end;
    none_stressed = @chain flattened_stress_df begin
        @by(:milestone_ids, :none_stressed = !any(:was_stressy))
        @rsubset(:none_stressed == true)
        @select(:milestone_ids, :stressed = false)
    end;
    return vcat(all_stressed, none_stressed, cols = :orderequal);
end

function fit_model(stress_df::AbstractDataFrame)
    stress_df_boolean_milestones = @chain stress_df begin
        @select(
            :name = :name,
            :milestone = .!ismissing.(:milestone_ids),
            :session,
            :was_stressy
        )
    end

    return Dict(
        group.name[1] => glm(@formula(was_stressy ~ session + milestone), group, Binomial(), LogitLink())
        for group in groupby(stress_df_boolean_milestones, :name)
    )
end

end
