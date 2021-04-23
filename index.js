/* eslint-disable quotes */

/*
1. Create this table in MySQL:
CREATE TABLE `T` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `count` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

2. Adjust script values numConcurrent and strategiesToTry.
3. Run with `DB_PASSWORD=password node index.js` and examine output for which strategies were correct or incorrect.

*/

const dbPassword = process.env.DB_PASSWORD;

const assert = require('assert');
const shuffle = require('lodash/shuffle');

const Sequelize = require('sequelize');
global.Promise = Sequelize.Promise;
const sequelize = new Sequelize(
  `mysql://sd:${dbPassword}@127.0.0.1:3311/sd_prod`
);
const T = sequelize.define(
  'T',
  {
    count: {
      type: Sequelize.INTEGER,
    },
  },
  { freezeTableName: true, timestamps: false, pool: { max: 2 } }
);

function getCaller() {
  return getCaller.caller;
}

const allExceptions = { noRow: [], oneRow: [] };
let exceptions = allExceptions.noRow;
const allRetries = { noRow: [], oneRow: [] };
let retries = allRetries.noRow;

async function reset(addOneRow = false) {
  await T.truncate({ restartIdentity: true });

  if (addOneRow) {
    const row = T.build({ count: 1 });
    await row.save();
  }

  const numRows = await T.count();
  assert.equal(
    numRows,
    addOneRow ? 1 : 0,
    `after reset, numRows should be ${addOneRow ? 1 : 0} but was ${numRows}`
  );
}

// const [txnIsolationLevel] = await sequelize.query(
//   'SELECT @@tx_isolation;',
//   { transaction }
// );
// console.log('txnIsolationLevel: ', txnIsolationLevel);

async function noLock(label, isolationLevel) {
  let transaction;
  try {
    transaction = await sequelize.transaction({
      isolationLevel,
    });
    const [row] = await T.findOrCreate({
      where: { id: 1 },
      transaction,
    });
    const newCount = row.count ? row.count + 1 : 1;
    await row.update({ count: newCount }, { transaction });
    await transaction.commit();
  } catch (ex) {
    exceptions.push({ strategy: getCaller(), exception: ex });
    console.error(`${label}: caught: ${ex}`);
    try {
      await transaction.rollback();
    } catch (innerEx) {
      if (
        /Transaction cannot be rolled back because it has been finished with state: rollback/i.test(
          innerEx
        )
      ) {
        console.log(`findOrCreate rolled back already`);
        // findOrCreate may roll back, so swallow if it has already rolled back.
      } else {
        console.log('DEBUG: throwing innerEx');
        throw innerEx;
      }
    }
  }
}
noLock.description = 'no lock, repeatable read';

async function lockForUpdate(label, isolationLevel) {
  let transaction;
  try {
    transaction = await sequelize.transaction({
      isolationLevel,
    });

    const [row] = await T.findOrCreate({
      where: { id: 1 },
      transaction,
      lock: transaction.LOCK.UPDATE,
    });
    const newCount = row.count ? row.count + 1 : 1;
    await row.update({ count: newCount }, { transaction });
    await transaction.commit();
  } catch (ex) {
    exceptions.push({ strategy: getCaller(), exception: ex });
    console.error(`${label}: caught: ${ex}`);
    await transaction.rollback();
  }
}
lockForUpdate.description = 'lock for update, repeatable read';

async function alwaysCreate(label, isolationLevel) {
  let transaction;
  try {
    transaction = await sequelize.transaction({
      isolationLevel,
    });

    const row = T.build({ id: 1 });
    await row.save({ transaction });
    await transaction.commit();
  } catch (ex) {
    exceptions.push({ strategy: getCaller(), exception: ex });
    // if row already exists, do nothing
    console.log(`${label}: caught ${ex}, doing nothing`);
    await transaction.rollback();
  } finally {
    // now row is guaranteed to exist
    // select old values,
    // update it and save

    // hypothesis: race condition here in that two interleaved transactions can
    // read the same old value, then increment to the same new value, when at
    // the end of both, it should have incremented twice?
    const transaction2 = await sequelize.transaction({ isolationLevel });
    const row = await T.findByPk(1, { transaction: transaction2 });
    const newCount = row.count ? row.count + 1 : 1;
    row.count = newCount;
    if (row.save) await row.save({ transaction: transaction2 });
    else console.log('no row found :(');
    await transaction2.commit();
  }
}
alwaysCreate.description = 'always create, repeatable read';

async function lockForUpdateRetry(label, isolationLevel) {
  async function lockForUpdateRetryTry() {
    let transaction;
    try {
      transaction = await sequelize.transaction({
        isolationLevel,
      });

      const [row] = await T.findOrCreate({
        where: { id: 1 },
        transaction,
        lock: transaction.LOCK.UPDATE,
      });
      const newCount = row.count ? row.count + 1 : 1;
      await row.update({ count: newCount }, { transaction });
      await transaction.commit();
    } catch (ex) {
      await transaction.rollback();
      throw ex;
    }
  }
  try {
    console.log(`${label}: first try`);
    await lockForUpdateRetryTry();
    return;
  } catch (ex) {
    console.error(`${label}: caught: ${ex}`);
    exceptions.push({ strategy: getCaller(), exception: ex });
    console.log(`retrying a second time`);
  }
  try {
    retries.push(1);
    console.log(`${label}: second try`);
    await lockForUpdateRetryTry();
    return;
  } catch (ex) {
    console.error(`${label}: caught: ${ex}`);
    exceptions.push({ strategy: getCaller(), exception: ex });
    console.log(`retrying a third time`);
  }
  try {
    retries.push(1);
    console.log(`${label}: third try`);
    await lockForUpdateRetryTry();
    return;
  } catch (ex) {
    console.error(`${label}: caught: ${ex}`);
    exceptions.push({ strategy: getCaller(), exception: ex });
    console.log(`retrying a fourth time`);
  }
  try {
    retries.push(1);
    console.log(`${label}: fourth try`);
    await lockForUpdateRetryTry();
    return;
  } catch (ex) {
    console.error(`${label}: caught: ${ex}`);
    exceptions.push({ strategy: getCaller(), exception: ex });
    exceptions.push({ strategy: getCaller(), exception: new Error('gave up') });
    console.log('gave up');
    throw ex;
  }
}
// this one works due to retries. would probably work with any txn level.
// so far, this is the only one I've seen be correct for both cases every time.
lockForUpdateRetry.description =
  'lock for update, repeatable read, with retries';

async function upsert(label, isolationLevel) {
  let transaction;
  try {
    await T.upsert({ id: 1 }, { where: { id: 1 } });
    // row is guaranteed to exist now

    try {
      transaction = await sequelize.transaction({
        isolationLevel,
      });

      const row = await T.findOne({
        where: { id: 1 },
        transaction,
        lock: transaction.LOCK.UPDATE,
      });
      const newCount = row.count ? row.count + 1 : 1;
      row.count = newCount;
      await row.save({ transaction });
      await transaction.commit();
    } catch (ex) {
      exceptions.push({ strategy: getCaller(), exception: ex });
      console.log(`${label}: caught ${ex}`);
      await transaction.rollback();
    }
  } catch (ex) {
    console.log('DEBUG: problem with upsert', ex);
  }
}

// this one fails unless the one before it failed, in which case this one
// doesn't run interleaved -- first txn runs to completion, so it succeeds
// increment5.description = 'lock for update, read comitted';

async function run(strategy, isolationLevel, numConcurrent) {
  const tasks = [];
  const start = 'A'.charCodeAt(0); // 65
  const end = start + numConcurrent - 1; // 65 + 2 - 1 = 66
  let current = start; // 65
  do {
    const task = strategy(String.fromCharCode(current), isolationLevel); //.catch(err => console.error(`a: ${err}`));
    tasks.push(task);
  } while (++current <= end);

  return await Promise.all(tasks).then(async () => {
    const numRows = await T.count();
    assert.equal(
      numRows,
      1,
      `after run, numRows should be 1 but was ${numRows}`
    );
    const row = await T.findByPk(1);
    const finalCount = row ? row.count : null;
    return finalCount;
  });
}

async function main() {
  await sequelize.authenticate().catch(err => {
    console.error('Unable to connect to the database:', err);
    throw err;
  });

  const numConcurrent = 4;
  const correctNoRow = numConcurrent;
  const correctOneRow = numConcurrent + 1;

  const strategiesToTry = shuffle([
    noLock,
    lockForUpdate,
    alwaysCreate,
    lockForUpdateRetry,
    upsert,
  ]);

  const isolationLevels = [
    Sequelize.Transaction.ISOLATION_LEVELS.REPEATABLE_READ,
    Sequelize.Transaction.ISOLATION_LEVELS.READ_COMMITTED,
  ];

  let correctStrategiesNoRow = [],
    incorrectStrategiesNoRow = [],
    correctStrategies1Row = [],
    incorrectStrategies1Row = [];

  try {
    console.log(`#`.repeat(60));
    console.log(
      `## no row already exists -- correct final count is ${correctNoRow} `
    );
    for (const strategy of strategiesToTry) {
      for (const isolationLevel of isolationLevels) {
        await reset();
        console.log(`\n-- ${strategy.name}, ${isolationLevel} ----------`);
        const finalCount = await run(strategy, isolationLevel, numConcurrent);

        let bucket = incorrectStrategiesNoRow;
        if (finalCount === correctNoRow) bucket = correctStrategiesNoRow;
        bucket.push({ strategy, finalCount, isolationLevel });

        console.log(`count: ${finalCount || 'no row found'}\n`);
      }
    }

    exceptions = allExceptions.oneRow;
    retries = allRetries.oneRow;

    console.log(`#`.repeat(60));
    console.log(
      `## a row already exists --  correct final count is ${correctOneRow} ##########`
    );
    for (const strategy of strategiesToTry) {
      for (const isolationLevel of isolationLevels) {
        await reset(true);
        console.log(`-- ${strategy.name}, ${isolationLevel} ----------`);
        const finalCount = await run(strategy, isolationLevel, numConcurrent);

        let bucket = incorrectStrategies1Row;
        if (finalCount === correctOneRow) bucket = correctStrategies1Row;
        bucket.push({ strategy, finalCount, isolationLevel });

        console.log(`count: ${finalCount || 'no row found'}\n`);
      }
    }
  } catch (ex) {
    console.log(`caught top-level exception, bailing:`);
    console.error(ex);
    process.exit(1);
  } finally {
    console.log(`#`.repeat(60));
    console.log(`## correct strategies   - no row already exists`);
    correctStrategiesNoRow.forEach(s =>
      console.log(`${s.strategy.name}, ${s.isolationLevel}`)
    );
    console.log(`\n## incorrect strategies - no row already exists ########`);
    incorrectStrategiesNoRow.forEach(s =>
      console.log(
        `${s.strategy.name}, ${s.isolationLevel} yielded ${s.finalCount} instead of ${correctNoRow}`
      )
    );
    console.log('\n## noRow retry count', allRetries.noRow.length);
    console.log(`\n## exceptions`);
    allExceptions.noRow.forEach(s =>
      console.log(`${s.strategy.name}: ${s.exception.message}`)
    );

    console.log(``);
    console.log(`#`.repeat(60));
    console.log(`## correct strategies   - 1 row already exists`);
    correctStrategies1Row.forEach(s =>
      console.log(`${s.strategy.name}, ${s.isolationLevel}`)
    );
    console.log(`\n## incorrect strategies - 1 row already exists ########`);
    incorrectStrategies1Row.forEach(s =>
      console.log(
        `${s.strategy.name}, ${s.isolationLevel} yielded ${s.finalCount} instead of ${correctOneRow}`
      )
    );

    console.log('\n## oneRow retry count', allRetries.oneRow.length);
    console.log(`\n## exceptions`);
    allExceptions.oneRow.forEach(s =>
      console.log(`${s.strategy.name}: ${s.exception.message}`)
    );
    process.exit(0);
  }
}

(async () => {
  await main();
})();
