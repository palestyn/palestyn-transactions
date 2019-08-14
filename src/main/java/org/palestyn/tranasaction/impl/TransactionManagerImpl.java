package org.palestyn.tranasaction.impl;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;
import javax.persistence.RollbackException;
import javax.transaction.Transactional;
import javax.transaction.Transactional.TxType;
import javax.transaction.TransactionalException;

import org.palestyn.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vavr.control.Either;
import io.vavr.control.Try;

public final class TransactionManagerImpl implements TransactionManager {
	
	private static Logger logger = LoggerFactory.getLogger(TransactionManagerImpl.class);

	@Inject
	private Instance<EntityManager> em;

	@Produces
	@RequestScoped
	private EntityManager getEntityManager() {
		EntityManager _em = null;
		try {
			EntityManagerFactory factory = (EntityManagerFactory) new InitialContext()
					.lookup("HibernateSessionFactory");
			_em = factory.createEntityManager();
		} catch (NamingException e) {
			throw new RuntimeException(e);
		}
		
		logger.debug("Returning new EntityManager for this request {}, {}", _em, _em.getProperties());
		return _em;
	}

	private Consumer<EntityManager> commit = em -> {
		logger.debug("Committing transaction");
		em.getTransaction().commit();			
	};
	
	private Consumer<EntityManager> close = em -> {
		logger.debug("Closing active EntityManager (session)");			
		em.close();
	};
	
	private BiConsumer<EntityManager, Boolean> rollback = (em, beforeCommit) -> {
		logger.debug("Transaction is rolled back" + (beforeCommit ? " before commit is called":""));
		em.getTransaction().rollback();
	};
	
	/**
	 * This will ensure that transaction is committed or rolled back and entity manager is closed.
	 * @param scope
	 * @param callback
	 */
	@Override
	public void doWithEntityManager(Transactional.TxType scope, Consumer<EntityManager> callback) {

		EntityManager _em = getScopedEntityManager(scope);
		
		Either<Throwable, Void> either = Try.run(() -> {
			callback.accept(_em);
		}).onFailure(PersistenceException.class, pe -> {
			rollback.accept(_em, true);
		}).andThenTry(() -> {
			commit.accept(_em);
		}).onFailure(RollbackException.class, rbe -> {
			rollback.accept(_em, false);
		}).andFinallyTry(() -> {
			close.accept(_em);
			em.destroy(_em);
		}).toEither();
		
		Throwable e = either.swap().getOrNull();
		if(Objects.nonNull(e)) {
			if(e instanceof PersistenceException)
				throw (PersistenceException)e;
			else throw new PersistenceException("Error while performing transaction, check the cause", e);
		}
	}
	
	@Override
	public <R> R doWithEntityManager(TxType scope, Function<EntityManager, R> callback) {
		
		EntityManager _em = getScopedEntityManager(scope);

		Either<Throwable, R> either = Try.ofSupplier(() -> {
			return callback.apply(_em);
		}).onFailure(PersistenceException.class, pe -> {
			rollback.accept(_em, true);
		}).andThenTry(() -> {
			commit.accept(_em);
		}).onFailure(RollbackException.class, rbe -> {
			rollback.accept(_em, false);
		}).andFinallyTry(() -> {
			close.accept(_em);
			em.destroy(_em);
		}).toEither();
		
		Throwable e = either.swap().getOrNull();
		
		if(Objects.nonNull(e)) {
			if(e instanceof PersistenceException)
				throw (PersistenceException)e;
			else throw new PersistenceException("Error while performing transaction, check the cause", e);
		}
		
		return either.get();
	}
	
	private EntityManager getScopedEntityManager(Transactional.TxType scope) {

		EntityManager em = this.em.get();

		switch (scope) {
		case REQUIRED:
			if (!em.getTransaction().isActive())
				em.getTransaction().begin();
			break;
		case MANDATORY:
			if (!em.getTransaction().isActive())
				throw new TransactionalException("Transaction is mandatory to be active", null);
			break;

		default:
			throw new RuntimeException(String.format("Transactional scope \"%s\" not supported", scope));
		}
		
		return em;
	}
}
