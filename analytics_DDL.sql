-- 0. analytics 스키마 지웠다가 생성
DROP SCHEMA IF EXISTS analytics CASCADE;
CREATE SCHEMA IF NOT EXISTS analytics;

-- 1. division_info 등급 테이블
CREATE TABLE analytics.division_info (
    division_id   INT          NOT NULL,      -- 등급번호 (PK)
    division_name VARCHAR(100) NOT NULL,      -- 등급명
    CONSTRAINT division_info_pk PRIMARY KEY (division_id)
);

-- 2. season_info 선수 시즌 정보 테이블
CREATE TABLE analytics.season_info (
    season_id  INT,                 -- 시즌_id (PK)
    name       VARCHAR(100),        -- 시즌명
    image_url  VARCHAR(200),        -- 시즌 아이콘 사진_url
    CONSTRAINT season_info_pk PRIMARY KEY (season_id)
);

-- 3. player_image_info: 선수 이미지 정보를 저장하는 테이블
CREATE TABLE analytics.player_image_info (
    spid INT NOT NULL,       -- 선수 고유 식별자 (PK)
    pid  INT,                -- 선수 고유 ID
    url  VARCHAR(255),       -- 선수 image 저장 장소
    CONSTRAINT player_image_info_pk PRIMARY KEY (spid)
);

-- 4. ranking_info 공식경기 랭킹 정보 테이블
CREATE TABLE analytics.ranking_info (
    gamer_nickname VARCHAR(100) NOT NULL,   -- 감독명 (PK)
    division_id    INT,                     -- 등급번호 (FK: division_info.division_id)
    gamer_level    INT,                     -- 게이머 레벨
    team_worth     BIGINT,                  -- 팀가치
    points         DECIMAL(10,2),           -- 승점
    winning_rate   DECIMAL(10,2),           -- 승률
    total_win      INT,                     -- 승리수
    total_draw     INT,                     -- 무승부수
    total_lose     INT,                     -- 패배수
    formation      VARCHAR(50),             -- 포메이션
    match_id       VARCHAR(50),             -- 매치 고유 번호
    created_at     Date,                    -- 랭킹 조회 시간
    ranking        INT,                     -- 순위
    CONSTRAINT ranking_info_pk PRIMARY KEY (gamer_nickname),
    CONSTRAINT ranking_info_fk_division_id FOREIGN KEY (division_id)
        REFERENCES analytics.division_info (division_id)
);

-- 5. match_info 세부 경기 정보 테이블
CREATE TABLE analytics.match_info (
    match_id        VARCHAR(50) NOT NULL,   -- 매치 고유 번호 (PK)
    spid            INT,                    -- 선수 고유 식별자 (FK: player_image_info.spid)
    season_id       INT,                    -- 시즌 ID (FK: season_info.season_id)
    gamer_nickname  VARCHAR(100),           -- 감독명 (FK: ranking_info.gamer_nickname)
    position        INT,                    -- 선수 포지션
    spGrade         INT,                    -- 선수 강화 레벨
    matchResult     VARCHAR(10),            -- 매치 결과 (예: 승, 무, 패)
    CONSTRAINT match_info_pk PRIMARY KEY (match_id),
    CONSTRAINT match_info_fk_spid FOREIGN KEY (spid)
        REFERENCES analytics.player_image_info (spid),
    CONSTRAINT match_info_fk_season_id FOREIGN KEY (season_id)
        REFERENCES analytics.season_info (season_id),
    CONSTRAINT match_info_fk_gamer_nickname FOREIGN KEY (gamer_nickname)
        REFERENCES analytics.ranking_info (gamer_nickname)
);

-- 6. team_color_info 팀컬러 정보 테이블
CREATE TABLE analytics.team_color_info (
	id              INT IDENTITY(1,1),      --인덱스
    gamer_nickname  VARCHAR(100),           -- 감독명
    team_color      VARCHAR(200),           -- 팀 컬러
    created_at      Date,                   -- 데이터 입력 시간
    CONSTRAINT team_color_info_pk PRIMARY KEY (id),
    CONSTRAINT team_color_info_fk_gamer_nickname FOREIGN KEY (gamer_nickname)
        REFERENCES analytics.ranking_info (gamer_nickname)
);

-- 7. player_review_info 선수 감정분석 결과 테이블
CREATE TABLE analytics.player_review_info (
    spid            INT,                    -- 선수 고유 식별자
    review          VARCHAR(200) NOT NULL,  -- 리뷰
    prediction      VARCHAR(100),           -- 선수 감정분석 결과
    CONSTRAINT player_review_info_pk PRIMARY KEY (spid, review),
    CONSTRAINT player_review_info_fk_spid FOREIGN KEY (spid)
        REFERENCES analytics.player_image_info (spid)
);
