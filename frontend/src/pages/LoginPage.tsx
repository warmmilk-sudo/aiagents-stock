import { FormEvent, useEffect, useState } from "react";
import { Navigate } from "react-router-dom";

import { ApiRequestError } from "../lib/api";
import { useAuthStore } from "../stores/authStore";
import styles from "./LoginPage.module.scss";


export function LoginPage() {
  const authenticated = useAuthStore((state) => state.authenticated);
  const lock = useAuthStore((state) => state.lock);
  const login = useAuthStore((state) => state.login);
  const hydrate = useAuthStore((state) => state.hydrate);
  const checking = useAuthStore((state) => state.checking);
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (checking) {
      void hydrate();
    }
  }, [checking, hydrate]);

  if (!checking && authenticated) {
    return <Navigate to="/research/deep-analysis" replace />;
  }

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setSubmitting(true);
    setError("");
    try {
      await login(password);
    } catch (requestError) {
      if (requestError instanceof ApiRequestError) {
        setError(requestError.message);
      } else {
        setError("登录失败");
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className={styles.page}>
      <div className={styles.panel}>
        <h1 className={styles.title}>aiagents stock</h1>
        <p className={styles.summary}>
        </p>
        <form onSubmit={handleSubmit}>
          <div className={styles.field}>
            <label htmlFor="password">管理员密码</label>
            <input
              id="password"
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              placeholder="请输入管理员密码"
            />
          </div>
          <div className={styles.actions}>
            <button className={styles.submit} disabled={submitting} type="submit">
              {submitting ? "登录中..." : "登录"}
            </button>
            {lock?.lock_until ? <span>锁定至 {new Date(lock.lock_until * 1000).toLocaleTimeString()}</span> : null}
          </div>
        </form>
        {error ? <p className={styles.error}>{error}</p> : null}
      </div>
    </div>
  );
}
