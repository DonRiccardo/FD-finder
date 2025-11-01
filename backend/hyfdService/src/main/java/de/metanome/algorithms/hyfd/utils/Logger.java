package de.metanome.algorithms.hyfd.utils;

/**
 * A simple singleton logger for logging messages to both the console
 * and an internal {@link StringBuilder} for later retrieval.
 */
public class Logger {

	private static Logger instance = null;
	
	private StringBuilder log = new StringBuilder();
	
	private Logger() {
	}

	/**
	 * Returns the singleton instance of the {@code Logger}.
	 *
	 * @return the singleton {@code Logger} instance
	 */
	public static Logger getInstance() {
		if (instance == null)
			instance = new Logger();
		return instance;
	}

	/**
	 * Appends a message to the log and prints it to the console.
	 *
	 * @param message the message to log
	 */
	public void write(String message) {
		this.log.append(message);
		System.out.print(message);
	}

	/**
	 * Appends a message with a newline to the log and prints it to the console.
	 *
	 * @param message the message to log
	 */
	public void writeln(String message) {
		this.log.append(message + "\r\n");
		System.out.println(message);
	}

	/**
	 * Logs an object by converting it to a string.
	 *
	 * @param message the object to log
	 */
	public void write(Object message) {
		this.write(message.toString());;
	}

	/**
	 * Logs an object with a newline by converting it to a string.
	 *
	 * @param message the object to log
	 */
	public void writeln(Object message) {
		this.writeln(message.toString());;
	}

	/**
	 * Returns the full content of the internal log as a string.
	 *
	 * @return the internal log content
	 */
	public String read() {

		return this.log.toString();
	}
}
