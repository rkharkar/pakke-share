module Importers

export read_boris_project

import JSON
using DataFrames
using Dates
using DataFramesMeta
using ThreadsX
using CSV

include("utils.jl")
using .Utils

function extract_subjects(prj_dict::Dict{String, Any})
    subjects_conf = try
        prj_dict["subjects_conf"];
    catch
        error("No subjects configured in project");
    end
    basic_df = Utils.build_dataframe(subjects_conf, ["name", "description"]);
    
    return DataFrame(
        id = 1:nrow(basic_df),
        name = basic_df.name,
        description = basic_df.description
    )
end

function extract_categories(prj_dict::Dict{String, Any})
    categories = try
        prj_dict["behavioral_categories"]
    catch
        error("No categories found in project");
    end

    return DataFrame(
        id = 1:length(categories),
        category = categories
    );
end

function extract_behaviors(prj_dict::Dict{String, Any}, categories_df::DataFrame)
    behaviors_conf = try
        prj_dict["behaviors_conf"]
    catch
        error("No behaviors found in project")
    end

    basic_df = Utils.build_dataframe(behaviors_conf, ["code", "type", "description", "category"]);

    behaviors_df = DataFrame(
        id = 1:nrow(basic_df),
        code = basic_df.code,
        type = basic_df.type,
        description = basic_df.description
    );

    category_map = Dict(row.category => row.id for row in eachrow(categories_df));

    behaviors_df.category_id = [
        ismissing(cat) ? missing : get(category_map, cat, missing)
        for cat in basic_df.category
            ];

    return behaviors_df;
end

function extract_subject_from_event(event::Vector, subjects::Set{String})
    for item in event
        if isa(item, String) && item in subjects
            return item;
        end
    end
end

function extract_observations(
    prj_dict::Dict{String, Any},
    subjects_df::DataFrame
    )
    if !haskey(prj_dict, "observations")
        error("No observations found in project");
    end
    if isempty(subjects_df)
        error("Subjects incorrectly specified");
    end

    subject_names = Set(subjects_df.name);
    subject_map = Dict(row.name => row.id for row in eachrow(subjects_df));

    observations = prj_dict["observations"];

    obs_ids = String[];
    dates = DateTime[];
    subject_names_vec = String[];
    durations = Float16[];
    
    for (obs_id, obs) in observations
        push!(obs_ids, obs_id);
        
        date_str = get(obs, "date", "");
        date = try
            date_str == "" ? missing : DateTime(date_str)
        catch
            missing
        end;
        push!(dates, date);

        obs_time_interval = obs["observation time interval"];
        media_length = 0;
        if obs_time_interval[2] != 0
            media_length = obs_time_interval[2] - obs_time_interval[1];
        else
            for (file, length) in obs["media_info"]["length"]
                media_length += length;
            end
        end

        push!(durations, media_length);

        subject_name = try
            extract_subject_from_event(obs["events"][1], subject_names)
        catch
            error("Not all events have subjects in $obs_id");
        end

        push!(subject_names_vec, subject_name);
    end
    
    subject_ids = [subject_map[name] for name in subject_names_vec];
    
    interim_df = DataFrame(
        obs_id = obs_ids,
        date = dates,
        subject_id = subject_ids,
        duration = durations
    );

    return @chain interim_df begin
        sort(_, :date)
        groupby(_, :subject_id)
        @transform(:session = eachindex(:date))
    end;
end

function extract_events(
    prj_dict::Dict{String, Any},
    observations_df::DataFrame,
    behaviors_df::DataFrame
    )
    observations = try
        prj_dict["observations"]
    catch
        error("No observations found in project");
    end
    
    behavior_map = Dict(row.code => row.id for row in eachrow(behaviors_df));
    behavior_types = Dict(row.code => row.type for row in eachrow(behaviors_df));

    all_events = [];

    for (obs_id_str, obs) in observations
        if !haskey(obs, "events") || isempty(obs["events"])
            continue;
        end

        valid_events = filter(e -> length(e) >= 3, obs["events"]);

        obs_events = map(valid_events) do event
            time = try
                convert(Float64, event[1])
            catch
                return nothing
            end;

            behavior_code = event[3];
            behavior_id = get(behavior_map, behavior_code, missing);

            if ismissing(behavior_id)
                return nothing;
            end

            behavior_type = get(behavior_types, behavior_code, "Unknown");

            return (
                obs_id = obs_id_str,
                behavior_id = behavior_id,
                time = time,
                type = behavior_type
            );
        end;

        filter!(e -> e !== nothing, obs_events)
        append!(all_events, obs_events)
    end

    if isempty(all_events)
        return DataFrame(
            obs_id = String[],
            behavior_id = Int[],
            time = Float64[],
            type = String[]
        );
    end

    events_df = DataFrame(
        obs_id = [e.obs_id for e in all_events],
        behavior_id = [e.behavior_id for e in all_events],
        time = [e.time for e in all_events],
        type = [e.type for e in all_events]
    );

    sort!(events_df, [:obs_id, :behavior_id, :time]);
    
    return events_df;
end

function construct_events_table(events_df::DataFrame)
    if isempty(events_df)
        return DataFrame(
            obs_id = String[],
            behavior_id = Int[],
            start = Float64[],
            stop = Union{Float64, Missing}[]
        );
    end

    state_events = @rsubset(events_df, :type .== "State event");
    point_events = @rsubset(events_df, :type .== "Point event");

    point_events.stop .= missing;
    renamed_points = rename(point_events, :time => :start);
    select!(renamed_points, :obs_id, :behavior_id, :start, :stop);

    if !isempty(state_events)
        groups = groupby(state_events, [:obs_id, :behavior_id])

        processed_groups = ThreadsX.map(groups) do group
            group.event_type = [isodd(i) ? "start" : "stop" for i in 1:nrow(group)];

            starts = @rsubset(group, :event_type .== "start");

            result = DataFrame(
                obs_id = starts.obs_id,
                behavior_id = starts.behavior_id,
                start = starts.time
            );

            stops = @rsubset(group, :event_type .== "stop");

            if nrow(stops) > 0
                result.stop = [i <= nrow(stops) ? stops.time[i] : missing for i in 1:nrow(result)];
            else
                result.stop = fill(missing, nrow(result));
            end

            return result;
        end

        collapsed_states = vcat(processed_groups...);
    else
        collapsed_states = DataFrame(
            obs_id = String[],
            behavior_id = Int[],
            start = Float64[],
            stop = Union{Float64, Missing}[]
        );
    end

    result = vcat(renamed_points, collapsed_states);

    sort!(result, [:obs_id, :start]);

    return result;
end

function extract_all_events(prj_dict::Dict{String, Any}, observations_df::DataFrame, behaviors_df::DataFrame)
    events_df = extract_events(prj_dict, observations_df, behaviors_df);

    final_events_df = construct_events_table(events_df);

    return final_events_df;
end

function map_milestones_to_sessions(raw_milestones, observations_df, milestones_map)
    observations_transformed = @chain observations_df begin
        @transform(
            :date_only = Date.(:date),
            :session_mapped = ifelse.(hour.(:date) .== 10, 1, 2)
        )
        @select(:obs_id, :date_only, :session_mapped, :session, :subject_id)
        @rename(:date = :date_only, :session_in_day = :session_mapped, :session_number = :session)
    end
    
    result = innerjoin(
        raw_milestones,
        observations_transformed,
        on = [:date, :subject_id, :session => :session_in_day]
    );
    
    return @chain result begin
        select(:obs_id, :milestone_id)
        combine(groupby(_, :obs_id)) do df
            return DataFrame(milestone_ids = [collect(df.milestone_id)])
        end
        leftjoin(observations_df, _, on = :obs_id)
        @orderby(:date)
    end
end

function import_milestones(filepath::String, subjects_df::DataFrame, observations_df::DataFrame)
    raw_milestones = DataFrame(CSV.File(filepath));
    
    unique_milestones = unique(raw_milestones.Event);
    milestones_df = DataFrame(
        id = 1:length(unique_milestones),
        milestone = unique_milestones
    );

    subjects_map = Dict(row.name => row.id for row in eachrow(subjects_df));
    milestones_map = Dict(row.milestone => row.id for row in eachrow(milestones_df));

    paired_values = [
        (milestones_map[row.Event], subjects_map[strip(row.Subject)])
        for row in eachrow(raw_milestones)
            ];
    
    milestones_vector = first.(paired_values);
    subjects_vector = last.(paired_values);

    raw_milestones[!, :milestone_id] = milestones_vector;
    raw_milestones[!, :subject_id] = subjects_vector;
    interim_df = select(
        raw_milestones,
        :subject_id,
        :milestone_id,
        :Date => :date,
        :Session => :session
    );
    return (milestones_df, map_milestones_to_sessions(interim_df, observations_df, milestones_map));
end

function read_boris_project(prj_filepath::String, milestones_filepath::String)
    println("Reading prt file");
    prj_dict = JSON.parsefile(prj_filepath);

    println("Extracting subjects");
    subjects_df = extract_subjects(prj_dict);
    println("Extracting categories");
    categories_df = extract_categories(prj_dict);

    println("Processing and populating behaviors");
    behaviors_df = extract_behaviors(prj_dict, categories_df);
    println("Processing and populating observations");
    observations_df = extract_observations(prj_dict, subjects_df);

    println("Building events");
    events_df = extract_all_events(prj_dict, observations_df, behaviors_df);

    println("Importing milestones");
    (milestones_df, observations_with_milestones_df) = import_milestones(milestones_filepath, subjects_df, observations_df);

    extracted_data = Dict(
        "subjects" => subjects_df,
        "categories" => categories_df,
        "behaviors" => behaviors_df,
        "observations" => observations_with_milestones_df,
        "events" => events_df,
        "milestones" => milestones_df
    );

    println("Finished building data frames");
    return extracted_data;
end

end # module
