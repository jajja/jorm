CREATE TEMPORARY TABLE locales (
    id          serial    NOT NULL,
    language    varchar   NOT NULL,
    country     varchar   NOT NULL,
    name        varchar,
    PRIMARY KEY (id),
    UNIQUE (language, country)
);

CREATE TEMPORARY TABLE tribes (
    id          serial    NOT NULL,
    name        varchar   NOT NULL    DEFAULT 'Greens', 
    PRIMARY KEY (id),
    UNIQUE (name)
);

CREATE TEMPORARY TABLE goblins (
    id          serial    NOT NULL,
    tribe_id    int       NOT NULL    REFERENCES tribes(id),
    name        varchar   NOT NULL, 
    mindset     varchar,
    PRIMARY KEY (id),
    UNIQUE (tribe_id, name)
);

CREATE TEMPORARY TABLE litters (
    id          serial    NOT NULL,
    goblin_id   int       NOT NULL    REFERENCES goblins(id),
    stench      float     NOT NULL    CHECK (stench BETWEEN 0 AND 1),
    left_at     timestamp NOT NULL    DEFAULT now(),
    PRIMARY KEY (id)
);

INSERT INTO locales (language, country) values
('sv', 'SE'),
('en', 'US'),
('da', 'DK'),
('no', 'NO'),
('fi', 'FI');

INSERT INTO tribes (name) values
('Snaga'),
('Uruk');

INSERT INTO goblins (tribe_id, name) values
(1, 'Azog'),
(1, 'Balcmeg'),
(1, 'Boldog'),
(1, 'Bolg'),
(1, 'Golfimbul'),
(1, 'Gorbag'),
(1, 'Gorgol'),
(1, 'The Great Goblin'),
(2, 'Grishnákh'),
(2, 'Lagduf'),
(1, 'Lug'),
(1, 'Lugdush'),
(1, 'Mauhúr'),
(1, 'Muzgash'),
(1, 'Orcobal'),
(1, 'Othrod'),
(1, 'Radbug'),
(1, 'Shagrat'),
(1, 'Snaga'),
(1, 'Ufthak'),
(2, 'Uglúk');

