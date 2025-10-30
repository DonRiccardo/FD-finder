import { Link, useMatch, useResolvedPath } from "react-router-dom"

/**
 * Create Navigatio bar to navigate between main pages.
 * @returns {JSX.Element} navigation bar
 */
export default function Navbar() {
    return (
        <nav className="navbar">
            <Link to="/" className="site-title">
                FD-Finder
            </Link>
            <ul>
                <CustomLink to="/datasets">Datasets</CustomLink>
                <CustomLink to="/jobs">Jobs</CustomLink>
                <CustomLink to="jobs/results">Job Results</CustomLink>
            </ul>
        </nav>
    );
}

/**
 * Create custom link for navigation bar.
 * @param {Object} props - input parameters
 * @param {string} props.to - link path
 * @param {JSX.Element} props.children - link children
 * @returns {JSX.ELement} custom link
 */
function CustomLink({ to, children, ...props }) {
  const resolvedPath = useResolvedPath(to)
  const isActive = useMatch({ path: resolvedPath.pathname, end: true })

  return (
    <li className={isActive ? "active" : ""}>
        <Link to={to} {...props}>
            {children}
        </Link>
    </li>
  )
}