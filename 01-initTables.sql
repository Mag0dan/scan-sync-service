CREATE TYPE user_role AS ENUM ('viewer', 'admin');

-- Миграция: создание таблиц организации
CREATE TABLE IF NOT EXISTS company (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    company_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS provided_ip (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    company_id INTEGER NOT NULL REFERENCES company (id) ON DELETE NO ACTION,
    ip_address inet NOT NULL,
    CONSTRAINT provided_ip_unique_idx UNIQUE (company_id, ip_address)
);


CREATE TABLE IF NOT EXISTS provided_domain (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    company_id INTEGER NOT NULL REFERENCES company (id) ON DELETE NO ACTION,
    domain_address VARCHAR(255) NOT NULL,
    CONSTRAINT provided_domain_unique_idx UNIQUE (company_id, domain_address)
);

CREATE TABLE IF NOT EXISTS provided_email (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    company_id INTEGER NOT NULL REFERENCES company (id) ON DELETE NO ACTION,
    email VARCHAR(255) NOT NULL,
    CONSTRAINT provided_email_unique_idx UNIQUE (company_id, email)
);

-- Миграция: создание таблицы пользователей
CREATE TABLE IF NOT EXISTS users (
    id  SERIAL  PRIMARY KEY,
    registered  TIMESTAMP   DEFAULT now() NOT NULL,
    login VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    totp_secret VARCHAR(64),
    totp_flag BOOL DEFAULT FALSE NOT NULL,
    company_id INTEGER NOT NULL REFERENCES company (id) ON DELETE NO ACTION,
    role user_role DEFAULT 'viewer' NOT NULL,
    failed_login_attempts INTEGER DEFAULT 0 NOT NULL,
    account_locked_until TIMESTAMP,
    must_change_password BOOLEAN DEFAULT TRUE NOT NULL,
    password_changed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
CREATE INDEX IF NOT EXISTS idx_users_company_role ON users(company_id, role);


-- Миграция: создание таблицы истории паролей
CREATE TABLE IF NOT EXISTS password_history (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_password_history_user_id ON password_history(user_id);
CREATE INDEX IF NOT EXISTS idx_password_history_created_at ON password_history(created_at);

-- Миграция: создание таблицы отчётов
CREATE TABLE IF NOT EXISTS report (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    company_id INTEGER REFERENCES company(id) ON DELETE NO ACTION
);

--IP
CREATE TABLE IF NOT EXISTS report_ip (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    report_id INTEGER REFERENCES report(id) ON DELETE NO ACTION,
    ip_address inet,
    port_number INTEGER,
    host_name VARCHAR(255),
    service_name VARCHAR(255),
    service_version VARCHAR(30),
    transport_protocol VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS report_subdomain (
    id SERIAL PRIMARY KEY,
    created        TIMESTAMP DEFAULT now() NOT NULL,
    report_ip_id   INTEGER REFERENCES report_ip(id) ON DELETE NO ACTION,
    sub_domain_ssl VARCHAR(255) NOT NULL,
    conn_type      VARCHAR(50)  NOT NULL,
    CONSTRAINT report_subdomain_unique
        UNIQUE (report_ip_id, sub_domain_ssl, conn_type)
);

CREATE TABLE IF NOT EXISTS encrypt_protocol (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    name VARCHAR(50) NOT NULL UNIQUE,
    description VARCHAR(255),
    crit_level REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS encryptProt_reportIp (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    report_ip_id INTEGER REFERENCES report_ip(id) ON DELETE NO ACTION,
    encrypt_protocol_id INTEGER REFERENCES encrypt_protocol(id) ON DELETE NO ACTION,
    CONSTRAINT encryptProt_reportIp_unique_idx UNIQUE (report_ip_id, encrypt_protocol_id)
);

CREATE TABLE IF NOT EXISTS vulnerability (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    cvss_score NUMERIC(3,1),
    cvss_version TEXT,
    cvss_vector TEXT
);

CREATE TABLE IF NOT EXISTS vulnerability_reportIp (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    report_ip_id INTEGER REFERENCES report_ip(id) ON DELETE NO ACTION,
    vulnerability_id INTEGER REFERENCES vulnerability(id) ON DELETE NO ACTION,
    CONSTRAINT vulnerability_reportIp_unique_idx UNIQUE (report_ip_id, vulnerability_id)
);

--SSL
CREATE TABLE IF NOT EXISTS report_ssl (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    report_id INTEGER REFERENCES report(id) ON DELETE NO ACTION,
    serial_number VARCHAR(255),
    cert_authority VARCHAR(255),
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    theme VARCHAR(255) NOT NULL,
    dns_names VARCHAR(255),
    has_foreign_domains BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT report_ssl_unique_idx UNIQUE (report_id, serial_number)
);


--Email
CREATE TABLE IF NOT EXISTS report_email (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    report_id INTEGER REFERENCES report(id) ON DELETE NO ACTION,
    email VARCHAR(255),
    compromised_flg BOOL DEFAULT FALSE NOT NULL,
    CONSTRAINT report_email_unique_idx UNIQUE (report_id, email)
);

CREATE TABLE IF NOT EXISTS search_loc (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    loc_name VARCHAR(255) NOT NULL,
    loc_date_from DATE NOT NULL,
    loc_date_to DATE NOT NULL,
    CONSTRAINT search_loc_unique_idx UNIQUE (loc_name, loc_date_from, loc_date_to)
);

CREATE TABLE IF NOT EXISTS report_email_loc (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    report_email_id INTEGER REFERENCES report_email(id) ON DELETE NO ACTION,
    search_loc_id INTEGER REFERENCES search_loc(id) ON DELETE NO ACTION,
    CONSTRAINT report_email_loc_unique_idx UNIQUE (report_email_id, search_loc_id)
);

CREATE TABLE IF NOT EXISTS leak (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    leak_name VARCHAR(255) NOT NULL,
    leak_date DATE NOT NULL,
    CONSTRAINT leak_unique_idx UNIQUE (leak_name, leak_date)
);


CREATE TABLE IF NOT EXISTS report_leak_email (
    id SERIAL PRIMARY KEY,
    created  TIMESTAMP   DEFAULT now() NOT NULL,
    report_email_id INTEGER REFERENCES report_email(id) ON DELETE NO ACTION,
    leak_id INTEGER REFERENCES leak(id) ON DELETE NO ACTION,
    CONSTRAINT report_leak_email_unique_idx UNIQUE (report_email_id, leak_id)
);

--Alike domain
CREATE TABLE IF NOT EXISTS report_alike_domain (
   id SERIAL PRIMARY KEY,
   created_at  TIMESTAMP   DEFAULT now() NOT NULL,
   report_id INTEGER REFERENCES report(id) ON DELETE NO ACTION,
   domain VARCHAR(255) NOT NULL,
   ip_address inet NOT NULL,
   CONSTRAINT report_alike_domain_unique_idx UNIQUE (report_id, domain, ip_address)
);