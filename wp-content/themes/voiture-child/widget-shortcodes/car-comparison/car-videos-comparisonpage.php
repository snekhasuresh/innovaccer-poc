<?php
function videos_shortcode($atts)
{
    $video_posts = get_car_videos_data()['posts'];
	$video_posts = array_slice($video_posts, 0, 3);

    if (empty($video_posts) || !is_array($video_posts)) {
        return;
    }

    ob_start();
?>
    <div class="youtube-videos-container">
        <h2 class="wa-title-text">Video Xe Ô Tô</h2>

        <?php foreach ($video_posts as $video_post) :
            $video_id = $video_post->video_youtube_id;
            $video_title = $video_post->post_title;
        ?>

            <div class="youtube-video" style="background-color: white; padding: 10px; margin-bottom: 20px; border: 1px solid #ccc; border-radius: 5px; cursor: pointer;" onclick="openModal('<?php echo esc_attr($video_id); ?>')">
                <div class="video-overlay">
                    <img src="https://img.youtube.com/vi/<?php echo esc_attr($video_id); ?>/hqdefault.jpg" width="100%" height="120" style="border-radius: 5px;" />
                </div>
                <h3 style="color: black; text-align: start; margin-top: 10px; font-size: 14px; line-height: 1.2; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-box-orient: vertical; -webkit-line-clamp: 2; white-space: normal; max-height: 3em;">
                    <?php echo esc_html($video_title); ?>
                </h3>
            </div>
        <?php endforeach; ?>
    </div>

    <!-- Modal Structure -->
    <div id="videoModal" class="modal">
        <div class="modal-content">
            <span class="close" onclick="closeModal()">&times;</span>
            <iframe id="videoFrame" width="100%" height="400px" frameborder="0" allow="autoplay; encrypted-media" allowfullscreen></iframe>
        </div>
    </div>

    <style>
        /* Basic Modal Styling */
        .modal {
            display: none;
            position: fixed;
            z-index: 1000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            overflow: auto;
            background-color: rgba(0, 0, 0, 0.8);
            /* Black background with opacity */
            justify-content: center;
            align-items: center;
        }

        .modal-content {
            background-color: #fefefe;
            margin: 5% auto;
            padding: 20px;
            border-radius: 8px;
            width: 80%;
            position: relative;
        }

        .close {
            position: absolute;
            right: 10px;
            top: 10px;
            font-size: 24px;
            color: #aaa;
            cursor: pointer;
        }

        .close:hover,
        .close:focus {
            color: black;
        }
    </style>

    <script>
        // Open the modal and play the video
        function openModal(videoId) {
            var modal = document.getElementById('videoModal');
            var videoFrame = document.getElementById('videoFrame');
            videoFrame.src = "https://www.youtube.com/embed/" + videoId + "?autoplay=1&controls=1";
            modal.style.display = "flex";
        }

        // Close the modal and stop the video
        function closeModal() {
            var modal = document.getElementById('videoModal');
            var videoFrame = document.getElementById('videoFrame');
            videoFrame.src = ""; // Clear the video frame src to stop the video
            modal.style.display = "none";
        }

        // Close modal if user clicks outside the modal content
        window.onclick = function(event) {
            var modal = document.getElementById('videoModal');
            if (event.target == modal) {
                closeModal();
            }
        }
    </script>

<?php
    wp_reset_postdata(); // Reset the query
    return ob_get_clean();
}

add_shortcode('youtube_videos-comparisonpage', 'videos_shortcode');
