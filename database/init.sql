create table update_data
(
    last_update_timestamp integer not null
);

create table universities
(
    name text not null,
    city text not null,
    constraint universities_pk
        primary key (name)
);

create table faculties
(
    id         integer not null,
    university text    not null,
    name       text    not null,
    constraint faculties_pk
        primary key (id),
    constraint faculties_universities_name_fk
        foreign key (university) references universities
);

create table group_types
(
    id   integer not null,
    name text    not null,
    constraint group_types_pk
        primary key (id)
);

create table category_types
(
    id   integer not null,
    name text    not null,
    constraint category_types_pk
        primary key (id)
);

create table directs
(
    id      integer not null,
    name    text    not null,
    form    text    not null,
    level   text    not null,
    faculty integer not null,
    constraint directs_pk
        primary key (id),
    constraint directs_faculties_id_fk
        foreign key (faculty) references faculties
);

create table groups
(
    id     integer not null,
    direct integer not null,
    type   integer not null,
    constraint groups_pk
        primary key (id),
    constraint groups_directs_id_fk
        foreign key (direct) references directs,
    constraint groups_group_types_id_fk
        foreign key (type) references group_types
);

create table categories
(
    id          integer not null,
    "group"     integer not null,
    type        integer not null,
    ctrl_number integer not null,
    constraint categories_pk
        primary key (id),
    constraint categories_groups_id_fk
        foreign key ("group") references groups,
    constraint categories_category_types_id_fk
        foreign key (type) references category_types
);

create table users
(
    id text not null,
    constraint users_pk
        primary key (id)
);

create table requests
(
    id           integer not null,
    rating       integer not null,
    "user"       text    not null,
    category     integer not null,
    total_sum    integer not null,
    original_doc boolean not null,
    constraint requests_pk
        primary key (id),
    constraint requests_categories_id_fk
        foreign key (category) references categories,
    constraint requests_users_id_fk
        foreign key ("user") references users
);

INSERT INTO update_data (last_update_timestamp)
VALUES (0);

INSERT INTO group_types (id, name)
VALUES (0, 'Бюджет'),
       (1, 'Договор'),
       (2, 'Целевое');

INSERT INTO category_types (id, name)
VALUES (0, 'Общий конкурс'),
       (1, 'Особое право'),
       (2, 'Специальная квота'),
       (3, 'БВИ'),
       (4, 'Иностранцы'),
       (5, 'Целевое');
