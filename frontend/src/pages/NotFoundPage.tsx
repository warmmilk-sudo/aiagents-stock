import { Link } from "react-router-dom";

import { PageFrame } from "../components/common/PageFrame";
import styles from "./ConsolePage.module.scss";


export function NotFoundPage() {
  return (
    <PageFrame title="页面不存在" summary="当前路由没有绑定页面，请回到控制台导航继续操作。">
      <div className={styles.card}>
        <Link className={styles.primaryButton} to="/research/deep-analysis">
          返回首页
        </Link>
      </div>
    </PageFrame>
  );
}
