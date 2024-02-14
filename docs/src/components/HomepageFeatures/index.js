import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: 'Declarative Table Management',
    Svg: require('@site/static/ui/feature_sql.svg').default,
    description: (
      <>
        Declaratively specify the table policies (retention,
        replication, sharing) using SQL APIs.
      </>
    ),
  },
  {
    title: 'Autonomous Data Services',
    Svg: require('@site/static/ui/feature_auto.svg').default,
    description: (
      <>
        Keeps the tables in managed (e.g., retention, replication),
optimal (e.g., storage compaction, sorting, clustering) and compliant state (e.g., GDPR).
      </>
    ),
  },
  {
    title: 'Secure Table Sharing',
    Svg: require('@site/static/ui/feature_secure.svg').default,
    description: (
      <>
        Provides a way to securely share the tables, with built in role-based
access control for table operations.
      </>
    ),
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
