import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import DocusaurusImageUrl from '@site/static/ui/openhouse_logo.png';

import Heading from '@theme/Heading';
import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <div class="row">
          <div class="col col--4 col--offset-1">
            <Heading as="h1" className={clsx('hero__title', styles.heroTitleStyle)}>
              {siteConfig.tagline}
            </Heading>
            <p className={clsx('hero__subtitle')}>{siteConfig.customFields.oneLineSummary}</p>
            <Link
                className={clsx('button button--secondary button--lg', styles.heroButton)}
                to="/docs/intro">
                Learn More
            </Link>
          </div>
          <div class="col col--4 col--offset-2">
            <img src={DocusaurusImageUrl} />
          </div>
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`${siteConfig.title}`}
      description={`${siteConfig.tagline}`}>
      <HomepageHeader />
      <main>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
