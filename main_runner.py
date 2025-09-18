import argparse
import logging
import os
import psycopg2
from dotenv import load_dotenv

from data.data_generators import (
    generate_customers,
    generate_products,
    generate_orders,
    generate_reviews,
    generate_shipments,
)

# Load environment variables
load_dotenv()

db_config = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}


def parse_args():
    p = argparse.ArgumentParser(description="Insert fake ecommerce data into the DB")
    grp = p.add_mutually_exclusive_group()
    grp.add_argument("--dry-run", action="store_true", help="Do not commit changes to the database")
    grp.add_argument("--commit", action="store_true", help="Explicitly commit changes (default if not --dry-run)")
    p.add_argument(
        "--only",
        type=str,
        default="all",
        help="Comma-separated list of datasets to generate: customers,products,orders,reviews,shipments or 'all'",
    )
    p.add_argument("--verbose", "-v", action="count", default=0, help="Increase verbosity")
    p.add_argument("--customers", type=int, default=50, help="Number of customers to generate")
    p.add_argument("--products", type=int, default=30, help="Number of products to generate")
    p.add_argument("--orders", type=int, default=100, help="Number of orders to generate")
    p.add_argument("--reviews", type=int, default=100, help="Number of reviews to generate")
    p.add_argument("--shipments", type=int, default=100, help="Number of shipments to generate")
    p.add_argument("--batch-size", type=int, default=100, help="Chunk size for batch inserts")
    p.add_argument("--sanity-check", action="store_true", help="Verify parent table counts before generating dependent datasets and abort if missing")
    # Use parse_known_args so that when pytest (or other tools) add flags, they
    # don't cause a SystemExit in unit tests that import and call main().
    args, _ = p.parse_known_args()
    return args


def main():
    args = parse_args()

    # configure logging
    level = logging.WARNING
    if args.verbose >= 2:
        level = logging.DEBUG
    elif args.verbose == 1:
        level = logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")

    only = [x.strip().lower() for x in args.only.split(",")] if args.only != "all" else ["all"]
    do_all = "all" in only

    report = {
        "customers": 0,
        "products": 0,
        "orders": 0,
        "order_items": 0,
        "reviews": 0,
        "shipments": 0,
    }

    logging.info("Connecting to database: %s@%s/%s", db_config.get("user"), db_config.get("host"), db_config.get("dbname"))
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            if args.sanity_check:
                problems = sanity_check(cur, only)
                if problems:
                    logging.error("Sanity check failed:\n%s", "\n".join(problems))
                    print("Sanity check failed; aborting. See log for details.")
                    return
            if do_all or "customers" in only:
                logging.info("Generating customers... count=%s", args.customers)
                generate_customers(cur, n=args.customers, report=report, chunk_size=args.batch_size)

            if do_all or "products" in only:
                logging.info("Generating products... count=%s", args.products)
                generate_products(cur, n=args.products, report=report, chunk_size=args.batch_size)

            if do_all or "orders" in only:
                logging.info("Generating orders... count=%s", args.orders)
                generate_orders(cur, n=args.orders, report=report, chunk_size=args.batch_size)

            if do_all or "reviews" in only:
                logging.info("Generating reviews... count=%s", args.reviews)
                generate_reviews(cur, n=args.reviews, report=report, chunk_size=args.batch_size)

            if do_all or "shipments" in only:
                logging.info("Generating shipments... count=%s", args.shipments)
                generate_shipments(cur, report=report, n=args.shipments, chunk_size=args.batch_size)

        if args.dry_run:
            conn.rollback()
            logging.info("Dry run: rolled back transaction")
        else:
            conn.commit()
            logging.info("Committed transaction")

    # Print a concise summary
    print("\nSummary:")
    for k, v in report.items():
        print(f"  {k}: {v}")

    print("Fake data inserted successfully." if not args.dry_run else "Dry run completed (no changes committed).")


if __name__ == "__main__":
    main()


def sanity_check(cur, only_list):
    """Return a list of human-readable problem strings; empty if OK.

    This considers which datasets will be generated in this run (only_list/do_all)
    and only checks parent tables that will NOT be generated in this run.
    """
    problems = []
    do_all = "all" in only_list

    def table_count(tbl_name):
        try:
            cur.execute(f"SELECT COUNT(*) FROM {tbl_name}")
            return cur.fetchone()[0]
        except Exception:
            return 0

    # If orders will be generated and customers/products are NOT being generated here,
    # ensure there are some existing rows to reference.
    will_gen_customers = do_all or ("customers" in only_list)
    will_gen_products = do_all or ("products" in only_list)
    will_gen_orders = do_all or ("orders" in only_list)
    will_gen_reviews = do_all or ("reviews" in only_list)
    will_gen_shipments = do_all or ("shipments" in only_list)

    # Orders require customers and products
    if will_gen_orders:
        if not will_gen_customers:
            ccount = table_count("Customers")
            if ccount <= 0:
                problems.append("Orders need Customers but Customers table is empty and will not be generated in this run.")
        if not will_gen_products:
            pcount = table_count("Products")
            if pcount <= 0:
                problems.append("Orders need Products but Products table is empty and will not be generated in this run.")

    # Reviews require customers and products
    if will_gen_reviews:
        if not will_gen_customers:
            ccount = table_count("Customers")
            if ccount <= 0:
                problems.append("Reviews need Customers but Customers table is empty and will not be generated in this run.")
        if not will_gen_products:
            pcount = table_count("Products")
            if pcount <= 0:
                problems.append("Reviews need Products but Products table is empty and will not be generated in this run.")

    # Shipments require orders
    if will_gen_shipments:
        if not will_gen_orders:
            ocount = table_count("Orders")
            if ocount <= 0:
                problems.append("Shipments need Orders but Orders table is empty and will not be generated in this run.")

    return problems
