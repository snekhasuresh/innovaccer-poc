jQuery(document).ready(function($) {
  // $('.pagination').on('click', '.prev-page, .next-page', function(e) {
  $(document).on('click', '.pagination .prev-page, .pagination .next-page', function(e) {
      e.preventDefault();

      var page = $(this).data('page');
      var category = $(this).data('category');
      var container = $(this).closest('.cate-news-container');
      // Add a Font Awesome spinner as the loader
      container.find('.loader').remove();
      container.find('.good-news-list').hide();
      container.find('.pagination').hide();
      container.append('<div class="loader"><i class="fas fa-spinner fa-spin"></br></i> Loading...</div>');

      homeUrl = window.location.origin;

      $.ajax({
          url: homeUrl + '/wp-admin/admin-ajax.php',
          type: 'POST',
          data: {
              action: 'cate_news_pagination',
              page: page,
              category: category,
              page_url: window.location.href 
          },
          success: function(response) {
              console.log('response', response);
              if (response.success) {
                  var tempDiv = $('<div>').html(response.data);

                  // Now use .find() to get the .good-news-list and .pagination
                  var newList = tempDiv.find('.good-news-list');
                  var newPagination = tempDiv.find('.pagination');;

                  // Make sure these elements are found and not empty
                  if (newList.length > 0) {
                      // $('.good-news-list').replaceWith(newList);
                      container.find('.good-news-list').replaceWith(newList);
                  }

                  if (newPagination.length > 0) {
                      // $('.pagination').replaceWith(newPagination);
                      container.find('.pagination').replaceWith(newPagination);
                  }
              } else {
                  console.error("Failed to load data.");
              }
          },
          error: function(xhr, status, error) {
              console.log('Error Status:', status);
              console.log('Error Response:', xhr.responseText); // Check the full response here
              console.log('Error:', error);
          },
          complete: function() {
              // Hide the loader and show the content and pagination again
              container.find('.loader').remove(); // Remove the loader
              container.find('.good-news-list').show(); // Show the content
              container.find('.pagination').show(); // Show the pagination
          }
      });
  });
});