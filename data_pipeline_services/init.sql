CREATE TABLE IF NOT EXISTS games (
  game_id VARCHAR PRIMARY KEY,
  game_date DATE NOT NULL,
  home_team VARCHAR(3),
  away_team VARCHAR(3),
  game_link TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS players (
  player_id SERIAL PRIMARY KEY,
  player_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS playerstats (
  game_id VARCHAR NOT NULL,
  player_id INTEGER NOT NULL,
  team VARCHAR(3) NOT NULL,
  opponent VARCHAR(3) NOT NULL,
  mp DOUBLE PRECISION,
  fg INTEGER,
  fga INTEGER,
  fg_percent DOUBLE PRECISION,
  three_p INTEGER,
  three_pa INTEGER,
  three_p_percent DOUBLE PRECISION,
  ft INTEGER,
  fta INTEGER,
  ft_percent DOUBLE PRECISION,
  orb INTEGER,
  drb INTEGER,
  trb INTEGER,
  ast INTEGER,
  stl INTEGER,
  blk INTEGER,
  tov INTEGER,
  pf INTEGER,
  pts INTEGER,
  gmsc DOUBLE PRECISION,
  plus_minus INTEGER,
  PRIMARY KEY (game_id, player_id),
  FOREIGN KEY (game_id) REFERENCES games(game_id),
  FOREIGN KEY (player_id) REFERENCES players(player_id)
);
