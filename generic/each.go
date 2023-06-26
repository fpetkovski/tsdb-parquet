package generic

import "sync"

func ParallelEach[T any](items []T, exec func(i int, item T) error) error {
	var (
		wg      sync.WaitGroup
		errChan = make(chan error, len(items))
	)
	wg.Add(len(items))
	for i, projection := range items {
		func(i int, projection T) {
			defer wg.Done()
			if err := exec(i, projection); err != nil {
				errChan <- err
			}
		}(i, projection)
	}
	wg.Wait()
	close(errChan)
	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil
}
