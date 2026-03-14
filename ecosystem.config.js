module.exports = {
    apps: [
        {
            name: "aiagents-stock",
            script: "venv/bin/python",
            args: "-m uvicorn backend.main:app --host 127.0.0.1 --port 8503 --no-access-log",
            cwd: "./",
            interpreter: "none",
            env: {
                NODE_ENV: "production",
            },
            restart_delay: 5000,
            max_restarts: 10,
            error_file: "./logs/pm2-error.log",
            out_file: "./logs/pm2-out.log",
            log_date_format: "YYYY-MM-DD HH:mm:ss"
        }
    ]
}
