create table public.universities
(
    name text not null
        constraint universities_pk
            primary key,
    city text not null
);

create table public.group_types
(
    name text not null
        constraint group_types_pk
            primary key
);

create table public.form_types
(
    name text not null
        constraint form_types_pk
            primary key
);

create table public.level_types
(
    name text not null
        constraint name
            primary key
);

create table public.directs
(
    university text not null
        constraint directs_universities_name_fk
            references public.universities,
    faculty    text not null,
    code       text not null,
    name       text not null,
    form       text
        constraint directs_form_types_name_fk
            references public.form_types,
    level      text
        constraint directs_level_types_name_fk
            references public.level_types,
    id         serial
        constraint directs_pk2
            unique,
    constraint directs_pk
        primary key (university, faculty, name)
);
alter table public.directs add constraint chk_code CHECK ( code ~* '\d\d\.\d\d\.\d\d' );

create table public.category_types
(
    name text not null
        constraint category_types_pk
            primary key
);

create table public.groups
(
    id            serial
        constraint groups_pk2
            unique,
    direct        integer not null
        constraint groups_directs_id_fk
            references public.directs (id),
    group_type    text    not null
        constraint groups_group_types_name_fk
            references public.group_types,
    category_type text    not null
        constraint groups_category_types_name_fk
            references public.category_types,
    ctrl_number   integer not null ,
    constraint groups_pk
        primary key (direct, group_type, category_type)
);

create table public.users
(
    snils   text not null
        constraint users_pk
            primary key,
    name    text,
    surname text
);

create table public.requests
(
    group_id     integer not null
        constraint requests_groups_id_fk
            references public.groups (id),
    "user"       text    not null
        constraint requests_users_snils_fk
            references public.users,
    rating       integer not null,
    total_sum    integer default 0,
    original_doc boolean default false,
    year         integer not null default extract(year from current_date),
    priority     integer not null default 1,
    constraint requests_pk
        primary key (group_id, "user")
);

create table public.passing_score
(
    direct     integer not null
        constraint passing_score_directs_id_fk
            references public.directs (id),
    group_type text    not null
        constraint passing_score_group_types_name_fk
            references public.group_types,
    score      integer not null,
    year       integer not null,
    constraint passing_score_pk
        primary key (direct, group_type, year)
);

create table update_data (
    last_update_timestamp   timestamp default 0
);

INSERT INTO group_types (name)
VALUES ('Бюджет'),
       ('Договор'),
       ('Целевое');

INSERT INTO category_types (name)
VALUES ('Общий конкурс'),
       ('Особое право'),
       ('Специальная квота'),
       ('БВИ'),
       ('Иностранцы'),
       ('Целевое');

INSERT INTO form_types (name)
VALUES ('Очно'),
       ('Заочно'),
       ('Очно/заочно');

INSERT INTO level_types (name)
VALUES ('Бакалавриат'),
       ('Магистратура'),
       ('Асперантура');