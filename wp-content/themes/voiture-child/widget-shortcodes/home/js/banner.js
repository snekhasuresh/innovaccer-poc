let index = 0;

function showSlides(n) {
    const slides = document.querySelectorAll('.carousel-slide');
    if (n >= slides.length) {
        index = 0;
    }
    if (n < 0) {
        index = slides.length - 1;
    }
    slides.forEach(slide => slide.style.display = 'none');
    slides[index].style.display = 'block';
}

// Auto-slide function
function autoSlide() {
    index++;
    showSlides(index);
}

// Event listeners for next and prev buttons
document.querySelector('.carousel-prev').addEventListener('click', () => {
    showSlides(--index);
});

document.querySelector('.carousel-next').addEventListener('click', () => {
    showSlides(++index);
});

// Initial slide show
showSlides(index);

// Set interval for auto slide (5 seconds in this case)
setInterval(autoSlide, 5000); // Adjust the interval as needed (5000ms = 5 seconds)