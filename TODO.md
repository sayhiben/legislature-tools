# Improvement ideas

- [ ] Add a "dark mode" theme to the website for better readability in low-light environments.
- [ ] Add a "zoom range" feature to the sidebar, allowing all timeseries charts to be zoomed in and out together. Sync the zoom level across all charts for better comparison, and update the zoom tool when the user zooms in on any individual chart.
- [ ] Add a "reset zoom" button to the sidebar, allowing users to quickly reset
- [ ] When the resolution/bucket size is changed, the page takes some time to redraw all the charts. Add a loading spinner or progress indicator to inform users that the charts are being updated, improving the user experience during this process.
- [ ] There's a mysterious "Ready" pill that appears on analysis sections. I'm not sure what a not-ready analysis would look like given the way we generate reports. This is probably not a useful label and we can safely remove it to reduce confusion.


# Bugs
- [ ] When the sidebar is expanded, the main content area is resized but the charts are not redrawn, resulting in distorted visuals. The charts should be redrawn when the sidebar is toggled to ensure they fit the new dimensions properly.