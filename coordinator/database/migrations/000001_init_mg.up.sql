create extension if not exists "uuid-ossp";

create table if not exists pipelines (
	pipeline_id UUID not null default gen_random_uuid() primary key,
	name varchar not null unique
);

create table if not exists steps (
	step_id UUID not null default gen_random_uuid() primary key,
	pipeline_id UUID not null,
	input_step_id UUID null default null,
	image varchar not null,
	runtime_config JSONB null default null,
	config JSONB null default null,
	name varchar not null,
	constraint steps_input_step_id foreign key (input_step_id) references steps(step_id),
	constraint steps_pipeline_id foreign key (pipeline_id) references pipelines(pipeline_id)
);

create table if not exists batches (
	batch_id UUID not null default gen_random_uuid() primary key,
	step_from_id UUID null default null,
	step_to_id UUID not null,
	issued_at timestamp not null default (now() at time zone 'utc'),
	uri varchar not null,
	constraint batches_step_from_id_fk foreign key (step_from_id) references steps(step_id),
	constraint batches_step_to_id_fk foreign key (step_to_id) references steps(step_id)
);

create table if not exists batch_process_log (
	correlation_id UUID not null default gen_random_uuid() primary key,
	batch_id UUID not null,
	started_at timestamp not null default (now() at time zone 'utc'),
	finished_at timestamp null default null,
	state varchar null default null,
	error varchar null default null,
	constraint batch_process_log_batch_id_fk foreign key (batch_id) references batches(batch_id)
);
