-- Create processed_files table if it doesn't exist
CREATE TABLE IF NOT EXISTS processed_files (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create unique index on file_name to prevent duplicates
CREATE UNIQUE INDEX IF NOT EXISTS idx_processed_files_file_name ON processed_files (file_name);
